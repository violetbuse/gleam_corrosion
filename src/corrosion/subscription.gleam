import corrosion/statement
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/uri

pub fn subscribe(
  corro_uri: uri.Uri,
  statement: statement.Statement,
  row_decoder: decode.Decoder(datatype),
) {
  todo
}
