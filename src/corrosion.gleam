import gleam/http/request
import gleam/option
import gleam/result
import gleam/uri

pub fn corro_uri(addr: String) -> Result(uri.Uri, Nil) {
  use uri <- result.try(uri.parse(addr))
  Ok(uri.Uri(..uri, path: "/", query: option.None, fragment: option.None))
}
