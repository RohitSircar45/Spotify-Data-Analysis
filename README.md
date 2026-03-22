# Spotify Data Analytics — SQL Business Intelligence

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Window%20Functions-00A878?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Complete-1DB954?style=for-the-badge)

A structured SQL analysis of the Spotify dataset using PostgreSQL window functions to answer 10 business intelligence questions across content strategy, artist performance, engagement, and streaming trends.

---

## Table of Contents

- [Dataset](#dataset)
- [Schema](#schema)
- [Exploratory Data Analysis](#exploratory-data-analysis)
- [Business Questions](#business-questions)
- [Window Function Reference](#window-function-reference)
- [Key SQL Patterns](#key-sql-patterns)
- [Setup](#setup)

---

## Dataset

The dataset contains track-level data from Spotify and YouTube, combining audio feature scores (from the Spotify API) with YouTube engagement metrics for the same tracks.

| Attribute | Detail |
|---|---|
| Grain | One row per track |
| Sources | Spotify API + YouTube Data API |
| Audio features | Danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, tempo |
| Engagement metrics | YouTube views, likes, comments |
| Stream data | Spotify stream count per track |

---

## Schema

```sql
CREATE TABLE spotify (
    artist            VARCHAR(255),
    track             VARCHAR(255),
    album             VARCHAR(255),
    album_type        VARCHAR(50),       -- 'Album', 'Single', 'Compilation'
    danceability      FLOAT,             -- 0.0 to 1.0
    energy            FLOAT,             -- 0.0 to 1.0
    loudness          FLOAT,             -- dB
    speechiness       FLOAT,             -- 0.0 to 1.0
    acousticness      FLOAT,             -- 0.0 to 1.0
    instrumentalness  FLOAT,             -- 0.0 to 1.0
    liveness          FLOAT,             -- 0.0 to 1.0
    valence           FLOAT,             -- 0.0 to 1.0
    tempo             FLOAT,             -- BPM
    duration_min      FLOAT,             -- track length in minutes
    title             VARCHAR(255),      -- YouTube video title
    channel           VARCHAR(255),      -- YouTube channel name
    views             FLOAT,             -- YouTube view count
    likes             BIGINT,            -- YouTube like count
    comments          BIGINT,            -- YouTube comment count
    licensed          BOOLEAN,
    official_video    BOOLEAN,
    stream            BIGINT,            -- Spotify stream count
    energy_liveness   FLOAT,             -- derived: energy * liveness
    most_played_on    VARCHAR(50)        -- platform with highest play share
);
```

---

## Exploratory Data Analysis

Before running business queries, the dataset was profiled and cleaned.

```sql
-- Row count and cardinality
SELECT COUNT(*) FROM spotify;
SELECT COUNT(DISTINCT album) FROM spotify;
SELECT DISTINCT album_type FROM spotify;

-- Duration outlier check
SELECT MAX(duration_min) FROM spotify;
SELECT MIN(duration_min) FROM spotify;

-- Remove corrupt zero-duration records
DELETE FROM spotify WHERE duration_min = 0;
```

Zero-duration tracks represent corrupt or incomplete records. Retaining them would skew any analysis involving track length, running totals, or ordered sequential comparisons.

---

## Business Questions

### Q1 — Most Viewed Track per Artist

```sql
SELECT artist, track, views
FROM (
    SELECT
        artist, track, views,
        RANK() OVER (PARTITION BY artist ORDER BY views DESC) AS rnk
    FROM spotify
) t
WHERE rnk = 1;
```

`RANK()` is used over `ROW_NUMBER()` because tied view counts should both surface — no peak track should be arbitrarily dropped.

---

### Q2 — Rank Albums by Total Streams per Artist

```sql
SELECT
    artist,
    album,
    total_streams,
    RANK() OVER (PARTITION BY artist ORDER BY total_streams DESC) AS rank
FROM (
    SELECT artist, album, SUM(stream) AS total_streams
    FROM spotify
    GROUP BY artist, album
) t;
```

Classic aggregate-then-rank pattern. The subquery collapses track-level rows to the artist-album grain before the window function is applied.

---

### Q3 — Top 3 Tracks by Energy per Artist

```sql
SELECT artist, track, energy
FROM (
    SELECT
        artist, track, energy,
        ROW_NUMBER() OVER (PARTITION BY artist ORDER BY energy DESC) AS rn
    FROM spotify
) t
WHERE rn <= 3;
```

`ROW_NUMBER()` guarantees exactly 3 results per artist. `RANK()` would return more than 3 rows if multiple tracks share the same energy value.

---

### Q4 — Most Engaging Track per Album

```sql
SELECT album, track, engagement
FROM (
    SELECT
        album, track,
        (likes + comments) AS engagement,
        RANK() OVER (
            PARTITION BY album
            ORDER BY (likes + comments) DESC
        ) AS rnk
    FROM spotify
) t
WHERE rnk = 1;
```

Engagement is computed inline as a derived column. `RANK()` correctly surfaces co-top tracks in the case of ties.

---

### Q5 — Rank Gap from Top Performer per Artist

```sql
SELECT
    artist,
    track,
    views,
    RANK() OVER (PARTITION BY artist ORDER BY views DESC) AS rnk,
    RANK() OVER (PARTITION BY artist ORDER BY views DESC) - 1 AS rank_gap
FROM spotify;
```

Two window expressions in the same `SELECT`. The second subtracts 1 to express distance from rank 1, avoiding a self-join. Tracks ranked first return a `rank_gap` of 0.

---

### Q6 — Previous Track's Stream Count

```sql
SELECT
    artist,
    track,
    stream,
    LAG(stream) OVER (
        PARTITION BY artist
        ORDER BY stream
    ) AS previous_stream
FROM spotify;
```

`LAG()` is a navigation function that accesses the preceding row within the partition. The first row per partition returns `NULL`. Foundation for computing stream deltas.

---

### Q7 — Running Total of Streams per Artist

```sql
SELECT
    artist,
    track,
    stream,
    SUM(stream) OVER (
        PARTITION BY artist
        ORDER BY stream DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM spotify;
```

The explicit `ROWS` frame is important. Without it, the default `RANGE` frame produces unexpected cumulative values when duplicate `stream` counts exist in the same partition.

---

### Q8 — Second Most Viewed Track per Artist

```sql
SELECT artist, track, views
FROM (
    SELECT
        artist, track, views,
        ROW_NUMBER() OVER (
            PARTITION BY artist
            ORDER BY views DESC
        ) AS rn
    FROM spotify
) t
WHERE rn = 2;
```

`ROW_NUMBER()` is required here. `RANK()` would skip position 2 if two tracks tie for first, jumping directly to rank 3.

---

### Q9 — Least Streamed Track per Artist

```sql
SELECT artist, track, stream, rn
FROM (
    SELECT
        artist, track, stream,
        ROW_NUMBER() OVER (
            PARTITION BY artist
            ORDER BY stream ASC
        ) AS rn
    FROM spotify
) t
WHERE rn = 1;
```

Mirrors Q8 with `ORDER BY stream ASC`. Same subquery architecture, inverted sort direction.

---

### Q10 — Danceability Ranking per Album

```sql
SELECT
    artist,
    album,
    track,
    danceability,
    DENSE_RANK() OVER (
        PARTITION BY album
        ORDER BY danceability DESC
    ) AS rnk
FROM spotify;
```

`DENSE_RANK()` produces no gaps after ties: if two tracks share rank 1, the next track receives rank 2 rather than 3. Appropriate for a continuous float metric where many values may be close after rounding.

---

## Window Function Reference

| Function | Tie Behaviour | Use When | Queries |
|---|---|---|---|
| `RANK()` | Skips ranks after ties (1, 1, 3) | Ties should both be visible; gaps acceptable | Q1, Q2, Q4, Q5 |
| `DENSE_RANK()` | No gaps after ties (1, 1, 2) | Continuous metrics with many similar values | Q10 |
| `ROW_NUMBER()` | Unique rank per row, arbitrary tie-break | Exactly N results per partition required | Q3, Q8, Q9 |
| `LAG()` | N/A | Compare current row to the preceding row | Q6 |
| `SUM()` windowed | N/A | Running or cumulative totals | Q7 |

---

## Key SQL Patterns

### Window functions cannot be filtered in WHERE

Window functions are evaluated after the `WHERE` clause. Filtering on a window result requires a subquery.

```sql
-- Invalid
SELECT artist, track, RANK() OVER (...) AS rnk
FROM spotify
WHERE rnk = 1;

-- Correct
SELECT artist, track
FROM (
    SELECT artist, track, RANK() OVER (...) AS rnk
    FROM spotify
) t
WHERE rnk = 1;
```

### Always define ROWS frame for aggregate windows

```sql
-- Ambiguous: default RANGE frame breaks on duplicate ORDER BY values
SUM(stream) OVER (PARTITION BY artist ORDER BY stream DESC)

-- Explicit: predictable behaviour regardless of duplicates
SUM(stream) OVER (
    PARTITION BY artist
    ORDER BY stream DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

### Aggregate then rank

When ranking aggregated values (e.g., total album streams), aggregate first in a subquery, then apply the window function in the outer query.

```sql
SELECT artist, album, total_streams,
    RANK() OVER (PARTITION BY artist ORDER BY total_streams DESC) AS rank
FROM (
    SELECT artist, album, SUM(stream) AS total_streams
    FROM spotify
    GROUP BY artist, album
) t;
```

---

## Setup

**Requirements:** PostgreSQL 12+

```bash
# 1. Create the database
createdb spotify_analysis

# 2. Connect and create the table
psql -d spotify_analysis -f schema.sql

# 3. Load your data (CSV)
psql -d spotify_analysis -c "\COPY spotify FROM 'spotify_data.csv' DELIMITER ',' CSV HEADER;"

# 4. Run EDA cleanup
psql -d spotify_analysis -c "DELETE FROM spotify WHERE duration_min = 0;"

# 5. Run queries
psql -d spotify_analysis -f queries.sql
```

> The dataset used in this project is sourced from Kaggle: [Spotify and Youtube](https://www.kaggle.com/datasets/salvatorerastelli/spotify-and-youtube).
