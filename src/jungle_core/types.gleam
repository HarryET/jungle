pub type CoreType {
  Int
  String
  Bool
  UUID
  Timestamp
  Unknown
}

pub fn decoder_str(type_: CoreType) {
  case type_ {
    Int -> "dynamic.int"
    String -> "dynamic.string"
    Bool -> "dynamic.bool"
    UUID -> "dynamic.dynamic"
    Timestamp -> "dynamic.dynamic"
    Unknown -> "dynamic.dynamic"
  }
}

pub fn to_pgo_cast(type_: CoreType) -> String {
  case type_ {
    Int -> "pgo.int"
    String -> "pgo.string"
    Bool -> "pgo.bool"
    UUID -> "jungle_core.pgo_panic"
    Timestamp -> "jungle_core.pgo_panic"
    Unknown -> "jungle_core.pgo_panic"
  }
}

pub fn to_core(pg: String) -> CoreType {
  case pg {
    "integer" -> Int
    "interval" -> Int
    "character varying" -> String
    "text" -> String
    "boolean" -> Bool
    "uuid" -> UUID
    "timestamp" -> Timestamp
    _ -> Unknown
  }
}

pub fn to_gleam_str(type_: CoreType, optional: Bool) -> String {
  let t = case type_ {
    Int -> "Int"
    String -> "String"
    Bool -> "Bool"
    UUID -> "dynamic.Dynamic"
    _ -> "dynamic.Dynamic"
  }

  case optional {
    True -> "Option(" <> t <> ")"
    False -> t
  }
}
