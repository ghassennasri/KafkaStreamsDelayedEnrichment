# Kafka Streams Enrichment Demo

This repo shows one way to enrich events in real-time with Kafka Streams, handling late-arriving enrtiching data. 
It compares a custom state store approach (the main solution) to a windowed join, proving why the former is beetter.

## What’s It About?

We’re enriching events from `event-topic` with data from `enrichment-topic`, outputting to `enriched-events-topic`. Late enrichments are tricky, so I use a custom state store to manage them.

### What it does
- **Handles Late Data**: Buffers events in a pending store until enrichments show up  —no strict time limits.
- **Single Output**: Sends exactly one enriched event, with control over timing.
- **Customizable Logic**: Lets you tweak the enrichment logic however you need.

## Why Not a Windowed Join?

The first solution (`createTopology`) is better than windowed join (`buildTopologyWithWindowedJoin`):
- **Late Data**: Pending store waits for enrichments; windows might drop events or repeat outputs.
- **Single Output**: One event, no duplicates—windows can get messy with updates.
- **Flexibility**: Fine-tune logic (see `EventEnrichmentTransformer`), unlike rigid join rules.

## Why not GlobalKTable?

While `GlobalKTable` might appear to be a convenient option for enrichment, it is not suitable for this use case due to several limitations:
- **Topic Clash**: Can’t use `enrichment-topic` for both a `GlobalKTable` and `KStream`—Kafka throws an error.
- **Too Basic**: Great for lookups, not for processing pending events like we need.
- **Locked In**: Limits how we handle late enrichments, unlike our `EnrichmentUpdateProcessor`.

## Code Highlights

- **Topology**: `createTopology` sets up stores for pending events and enrichments.
- **Transformer**: `EventEnrichmentTransformer` tries enriching right away or waits, with a timer to check later.
- **Processor**: `EnrichmentUpdateProcessor` updates enrichments and matches pending events.
- **Alternative**: `buildTopologyWithWindowedJoin` shows the windowed join option.

3. **Test it**
    - Check `KafkaStreamsEnrichmentAppTest` for instant and delayed enrichment examples (unit test).
    - Check `KafkaStreamsEnrichmentAppIntegrationTest` for a full integration test with Kafka.
    - Run tests with `./gradlew test --tests [testName]`.

