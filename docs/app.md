## scoring system

- more time only earn more score: [onlTime, offlTime]
- more session established, more score earned
- more different pair (new student, new expert) more scores earned => payless
  users have lower chance => activities |> score (event sourcing here!!!)

## student can pick direct expert

- with init offer. Problem with this, system charge session fee
  - what if student and expert consensus a very low fee?
  - what if student and expert have contact of each other => solve by scoring
    system

## bidding system

- lower bid, higher chance
- higher score, higher chance
- quicker give the bid, higher chance
- more related native language, higher chance => trade-off: more metric, more
  bias?!

## tuning-able

- system just offer metrics to be configured
- tuning metrics need to be done by business experts, not at the time system
  developed
- ranking, scoring can be configured
  - list metrics to check can be configured
  - metric's scores can be configured

## tracing-able

- ranking, scoring alg need to be versioned
- session established | score updated event info include ranking alg version was
  used

## how AI help?!

- AI help more FASTER and more ACCURATE
  - faster score fetching
  - faster subject detection
  - faster problem detection <|
  - faster OCR <|
  - lighter model
