import corrosion/internal/query_event
import corrosion/internal/util
import corrosion/statement
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/http/request
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import gleam/uri
import httpp/send

pub fn query(
  corro_uri: uri.Uri,
  statement: statement.Statement,
  row_decoder: decode.Decoder(datatype),
) -> Result(List(datatype), String) {
  let uri = uri.Uri(..corro_uri, path: "/v1/queries")
  let body =
    util.statement_to_json(statement)
    |> json.to_string_tree
    |> bytes_tree.from_string_tree

  let assert Ok(base_request) = request.from_uri(uri)
  let request =
    base_request
    |> request.set_header("content-type", "application/json")
    |> request.set_body(body)

  use response <- result.try(
    send.send_bits(request)
    |> result.replace_error("Failed to send request to server."),
  )
  use string <- result.try(
    bit_array.to_string(response.body)
    |> result.replace_error("Server did not send utf-8 text back."),
  )

  let events =
    string
    |> string.split("\n")
    |> list.map(string.trim)
    |> list.filter(fn(line) { string.is_empty(line) |> bool.negate })
    |> list.map(fn(line) {
      case json.parse(line, query_event.decoder()) {
        Ok(evt) -> Ok(evt)
        Error(_) -> Error("Failed to parse line from server: " <> line)
      }
    })
    |> result.all

  use events <- result.try(events)

  let event_handler = query_event.new_event_handler(row_decoder)
  use applied_events <- result.try(
    list.fold(events, Ok(event_handler), fn(acc, event) {
      use handler <- result.try(acc)
      query_event.handle_event(handler, event)
    }),
  )

  use rows <- result.try(
    query_event.get_sorted(applied_events)
    |> result.replace_error("Server did not send an end-of-query event."),
  )

  Ok(rows)
}
