schema_str = """{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "latest trending movie",
  "type": "object",
  "properties": {
    "adult": {
      "type": "boolean"
    },
    "backdrop_path": {
      "type": "string"
    },
    "genre_ids": {
      "type": "array",
      "items": {
        "type": "number"
      }
    },
    "id": {
      "type": "number"
    },
    "media_type": {
      "type": "string"
    },
    "original_language": {
      "type": "string"
    },
    "original_title": {
      "type": "string"
    },
    "overview": {
      "type": "string"
    },
    "popularity": {
      "type": "number"
    },
    "poster_path": {
      "type": "string"
    },
    "release_date": {
      "type": "string"
    },
    "title": {
      "type": "string"
    },
    "video": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "vote_average": {
      "type": "number"
    },
    "vote_count": {
      "type": "number"
    }
  },
  "required": [
    "adult",
    "backdrop_path",
    "genre_ids",
    "id",
    "media_type",
    "original_language",
    "original_title",
    "overview",
    "popularity",
    "poster_path",
    "release_date",
    "title",
    "video",
    "vote_average",
    "vote_count"
  ]
}"""