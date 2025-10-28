import corrosion/internal/util
import corrosion/query_event
import corrosion/statement
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dynamic
import gleam/dynamic/decode
import gleam/http/request
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import gleam/uri
import httpp/send

pub type QueryResult(datatype) {
  QueryResult(rows: List(datatype), time: Float)
}

pub fn query(
  corro_uri: uri.Uri,
  statement: statement.Statement,
  row_decoder: decode.Decoder(datatype),
) -> Result(QueryResult(datatype), String) {
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

  map_events(events, [], row_decoder)
}

fn map_events(
  events: List(query_event.QueryEvent),
  columns: List(String),
  decoder: decode.Decoder(datatype),
) -> Result(QueryResult(datatype), String) {
  use <- bool.guard(
    when: list.is_empty(events),
    return: Ok(QueryResult(rows: [], time: 0.0)),
  )
  let assert [event, ..remaining_events] = events

  case event {
    query_event.Columns(cols) -> map_events(remaining_events, cols, decoder)
    query_event.Row(rowid: _, values:) -> {
      let zipped =
        list.map(columns, dynamic.string)
        |> list.strict_zip(values)
        |> result.map(dynamic.properties)
        |> result.replace_error(
          "Columns array is a different size from values.",
        )

      use dynamic <- result.try(zipped)

      let decode_result =
        decode.run(dynamic, decoder)
        |> result.try_recover(fn(_) {
          decode.run(dynamic.list(values), decoder)
        })
        |> result.replace_error("Could not decode value from server.")

      use value <- result.try(decode_result)

      case map_events(remaining_events, columns, decoder) {
        Ok(mapped) -> Ok(QueryResult(..mapped, rows: [value, ..mapped.rows]))
        error -> error
      }
    }
    query_event.EOQ(time:, change_id: _) -> Ok(QueryResult(rows: [], time:))
    query_event.Change(..) -> Error("/v1/query received a change event.")
    query_event.QueryError(message:) -> Error(message)
  }
}
