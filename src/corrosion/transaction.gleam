import corrosion/internal/util
import corrosion/statement
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/http/request
import gleam/json
import gleam/option
import gleam/result
import gleam/uri
import httpp/hackney
import httpp/send

pub type ExecResult {
  Execute(rows_affected: Int, time: Float)
  Error(error: String)
}

pub type ExecResponse {
  ExecResponse(
    results: List(ExecResult),
    time: Float,
    version: option.Option(Int),
    actor_id: option.Option(String),
  )
}

pub fn transact(
  corro_uri: uri.Uri,
  statements: List(statement.Statement),
) -> Result(ExecResponse, hackney.Error) {
  let decoder = {
    let exec_result_decoder = {
      let execute_decoder = {
        use rows_affected <- decode.field("rows_affected", decode.int)
        use time <- decode.field("time", decode.float)

        decode.success(Execute(rows_affected:, time:))
      }

      let error_decoder = {
        use error <- decode.field("error", decode.string)

        decode.success(Error(error:))
      }

      decode.one_of(execute_decoder, [error_decoder])
    }

    use results <- decode.field("results", decode.list(exec_result_decoder))
    use time <- decode.field("time", decode.float)
    use version <- decode.optional_field(
      "version",
      option.None,
      decode.optional(decode.int),
    )
    use actor_id <- decode.optional_field(
      "actor_id",
      option.None,
      decode.optional(decode.string),
    )

    decode.success(ExecResponse(results:, time:, version:, actor_id:))
  }

  let uri = uri.Uri(..corro_uri, path: "/v1/transactions")
  let body =
    json.array(statements, util.statement_to_json)
    |> json.to_string_tree
    |> bytes_tree.from_string_tree

  let assert Ok(base_request) = request.from_uri(uri)
  let request =
    base_request
    |> request.set_header("content-type", "application/json")
    |> request.set_body(body)

  use result <- result.try(send.send_bits(request))
  let assert Ok(response) = json.parse_bits(result.body, decoder)

  Ok(response)
}
