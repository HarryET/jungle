// Core Functions for Utilities
import gleam/dynamic

pub fn all_errors(
  result: Result(a, List(dynamic.DecodeError)),
) -> List(dynamic.DecodeError) {
  case result {
    Ok(_) -> []
    Error(errors) -> errors
  }
}

pub fn pgo_panic(_: a) {
  // Sometimes stuff hasn't been implemented e.g. UUIDs in pgo so we have to panic!
  // It's not you, its me; we will have to go our separate ways for now...
  panic
}
