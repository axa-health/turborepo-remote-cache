import { Writable } from 'node:stream'
import fetch from 'node-fetch'
import logger from '../../../logger.js'
import { StorageProvider } from './index.js'

const VERSION = '10'

export function createGithubActionsCache(): StorageProvider {
  const cache = GithubActionsCache.fromEnv()

  return {
    createReadStream(artifactPath: string): Promise<NodeJS.ReadableStream> {
      return cache.get([artifactPath], VERSION)
    },
    createWriteStream(artifactPath: string): Promise<Writable> {
      return cache.upload(artifactPath, VERSION)
    },
    exists(
      artifactPath: string,
      cb: (err: Error | null, exists?: boolean) => void,
    ): void {
      cache
        .getMeta([artifactPath], VERSION)
        .then(() => cb(null, true))
        .catch((err) => {
          if (err instanceof NotFoundError) {
            cb(null, false)
          } else {
            cb(err, false)
          }
        })
    },
  }
}

class NotFoundError extends Error {}

class GithubActionsCache {
  constructor(private url: string, private token: string) {}

  static fromEnv(): GithubActionsCache {
    if (!process.env.ACTIONS_CACHE_URL) {
      throw new Error('missing ACTIONS_CACHE_URL')
    }
    if (!process.env.ACTIONS_RUNTIME_TOKEN) {
      throw new Error('missing ACTIONS_RUNTIME_TOKEN')
    }
    return new GithubActionsCache(
      process.env.ACTIONS_CACHE_URL,
      process.env.ACTIONS_RUNTIME_TOKEN,
    )
  }

  async getMeta(
    keys: string[],
    version: string,
  ): Promise<{ archiveLocation: string }> {
    logger.info(`Getting ${keys} ${version}`)
    const res = await fetch(
      `${this.url}_apis/artifactcache/cache?keys=${encodeURIComponent(
        keys.join(','),
      )}&version=${encodeURIComponent(version)}`,
      {
        headers: {
          Accept: 'application/json;api-version=6.0-preview.1',
          Authorization: `Bearer ${this.token}`,
        },
      },
    )
    if (!res.ok) {
      throw new NotFoundError()
    }
    const body = (await res.json()) as { archiveLocation: string }
    if (!body.archiveLocation) {
      throw new NotFoundError() // idk if this is possible
    }
    return body
  }

  async get(keys: string[], version: string): Promise<NodeJS.ReadableStream> {
    const meta = await this.getMeta(keys, version)

    const res = await fetch(meta.archiveLocation)
    if (!res.ok || !res.body) {
      throw new Error('Failed to fetch blob')
    }
    return res.body
  }

  async upload(key: string, version: string): Promise<Writable> {
    logger.info(`Uploading ${key} ${version}`)

    const res = await fetch(`${this.url}_apis/artifactcache/caches`, {
      method: 'POST',
      headers: {
        Accept: 'application/json;api-version=6.0-preview.1',
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.token}`,
      },
      body: JSON.stringify({
        key,
        version,
      }),
    })
    if (!res.ok) {
      throw new Error(await res.text())
    }
    const body = (await res.json()) as { cacheId: string }
    const id = body.cacheId

    return new Upload(id, this.url, this.token)
  }
}

class Upload extends Writable {
  private data: Uint8Array

  constructor(
    private cacheId: string,
    private url: string,
    private token: string,
  ) {
    super()
    this.data = Buffer.of()
  }

  _write(
    // biome-ignore lint:lint/suspicious/noExplicitAny
    chunk: any,
    _: BufferEncoding,
    callback: (error?: Error | null) => void,
  ) {
    this.data = Buffer.concat([this.data, chunk])
    callback(null)
  }

  _final(callback: (error?: Error | null) => void) {
    this.completeUpload()
      .then(() => callback(null))
      .catch((err) => callback(err))
  }

  async completeUpload() {
    // https://github.com/actions/toolkit/blob/7f5921cdddc31081d4754a42711d71e7890b0d06/packages/cache/src/options.ts#L81
    const CHUNK_SIZE = 32 * 1024 * 1024
    const chunks = Math.ceil(this.data.length / CHUNK_SIZE)

    for (let i = 0; i < chunks; i++) {
      const chunk = this.data.slice(
        i * CHUNK_SIZE,
        Math.min((i + 1) * CHUNK_SIZE, this.data.length),
      )

      logger.info(`Completing uploading chunk ${i}`)
      const res = await fetch(
        `${this.url}_apis/artifactcache/caches/${encodeURIComponent(
          this.cacheId,
        )}`,
        {
          method: 'PATCH',
          body: chunk,
          headers: {
            Accept: 'application/json;api-version=6.0-preview.1',
            'Content-Type': 'application/octet-stream',
            'Content-Range': `bytes ${i * CHUNK_SIZE}-${
              i * CHUNK_SIZE + chunk.length - 1
            }/${this.data.length}`,
            Authorization: `Bearer ${this.token}`,
          },
        },
      )
      if (!res.ok) {
        throw new Error(
          `Failed to patch upload range for upload: ${await res.text()}`,
        )
      }
    }

    logger.info('Completing upload for artifact')

    const res = await fetch(
      `${this.url}_apis/artifactcache/caches/${encodeURIComponent(
        this.cacheId,
      )}`,
      {
        method: 'POST',
        body: JSON.stringify({
          size: this.data.length,
        }),
        headers: {
          Accept: 'application/json;api-version=6.0-preview.1',
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.token}`,
        },
      },
    )
    if (!res.ok) {
      throw new Error(`Failed to finalize POST for upload: ${await res.text()}`)
    }
  }
}
