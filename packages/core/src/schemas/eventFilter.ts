import { z } from "zod";

export const stringPatternMatchers = [
  z.object({
    $endsWith: z.string(),
  }),
  z.object({
    $startsWith: z.string(),
  }),
  z.object({
    $ignoreCaseEquals: z.string(),
  }),
] as const;

const EventMatcherSchema = z.union([
  /** Match against a string */
  z.array(z.string()),
  /** Match against a number */
  z.array(z.number()),
  /** Match against a boolean */
  z.array(z.boolean()),
  z.array(
    z.union([
      ...stringPatternMatchers,
      z.object({
        $exists: z.boolean(),
      }),
      z.object({
        $isNull: z.boolean(),
      }),
      z.object({
        $anythingBut: z.union([z.string(), z.number(), z.boolean()]),
      }),
      z.object({
        $anythingBut: z.union([z.array(z.string()), z.array(z.number()), z.array(z.boolean())]),
      }),
      z.object({
        $gt: z.number(),
      }),
      z.object({
        $lt: z.number(),
      }),
      z.object({
        $gte: z.number(),
      }),
      z.object({
        $lte: z.number(),
      }),
      z.object({
        $between: z.tuple([z.number(), z.number()]),
      }),
      z.object({
        $includes: z.union([z.string(), z.number(), z.boolean()]),
      }),
    ])
  ),
]);

type EventMatcher = z.infer<typeof EventMatcherSchema>;

type BaseContentMatcher =
  | {
      $exists: boolean;
    }
  | {
      $isNull: boolean;
    };

type StringContentMatcher =
  | BaseContentMatcher
  | { $endsWith: string }
  | { $startsWith: string }
  | { $anythingBut: string | string[] }
  | { $ignoreCaseEquals: string }
  | { $includes: string };

type StringMatcher = string[] | StringContentMatcher[];

type NumberContentMatcher =
  | BaseContentMatcher
  | { $anythingBut: number | number[] }
  | { $gt: number }
  | { $lt: number }
  | { $gte: number }
  | { $lte: number }
  | { $between: [number, number] }
  | { $includes: number };

type NumberMatcher = number[] | NumberContentMatcher[];

type BooleanContentMatcher =
  | BaseContentMatcher
  | { $anythingBut: boolean | boolean[] }
  | { $includes: boolean };

type BooleanMatcher = boolean[] | BooleanContentMatcher[];

type GenericEventMatcher<T> = NonNullable<T> extends string
  ? StringMatcher
  : NonNullable<T> extends number
  ? NumberMatcher
  : NonNullable<T> extends boolean
  ? BooleanMatcher
  : EventMatcher;

/** A filter for matching against data */
export type EventFilter<TEvent extends any = any> = TEvent extends Record<string, any>
  ? {
      [K in keyof TEvent]?: TEvent[K] extends Record<string, any>
        ? EventFilter<TEvent[K]>
        : GenericEventMatcher<TEvent[K]>;
    }
  : {
      [key: string]: EventFilter | EventMatcher;
    };

export const EventFilterSchema: z.ZodType<EventFilter> = z.lazy(() =>
  z.record(z.union([EventMatcherSchema, EventFilterSchema]))
);

export const EventRuleSchema = z.object({
  event: z.string().or(z.array(z.string())),
  source: z.string(),
  payload: EventFilterSchema.optional(),
  context: EventFilterSchema.optional(),
});

export type EventRule = z.infer<typeof EventRuleSchema>;
