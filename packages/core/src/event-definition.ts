export interface EventDefinition<TPayload = unknown> {
  readonly name: string
  readonly version: string
  readonly _phantom?: TPayload
}

export function defineEvent<TPayload>(name: string, version: string): EventDefinition<TPayload> {
  return { name, version }
}

export type PayloadOf<T extends EventDefinition> = T extends EventDefinition<infer P> ? P : never
