import corrosion/statement
import gleam/function
import gleam/json

@external(erlang, "erlang", "binary_to_list")
fn binary_to_list(input: BitArray) -> List(Int)

pub fn parameter_to_json(param: statement.Parameter) -> json.Json {
  case param {
    statement.Null -> json.null()
    statement.Blob(bits) -> binary_to_list(bits) |> json.array(json.int)
    statement.Bool(bool) -> json.bool(bool)
    statement.Integer(int) -> json.int(int)
    statement.Real(real) -> json.float(real)
    statement.Text(text) -> json.string(text)
  }
}

pub fn statement_to_json(statement: statement.Statement) -> json.Json {
  case statement {
    statement.Simple(query) -> json.string(query)
    statement.Verbose(query:, params:, named_params:) ->
      json.object([
        #("query", json.string(query)),
        #("params", json.nullable(params, json.array(_, parameter_to_json))),
        #(
          "named_params",
          json.nullable(named_params, json.dict(
            _,
            function.identity,
            parameter_to_json,
          )),
        ),
      ])
    statement.WithNamedParams(query, named_params) ->
      json.preprocessed_array([
        json.string(query),
        json.dict(named_params, function.identity, parameter_to_json),
      ])
    statement.WithParams(query, params) ->
      json.preprocessed_array([
        json.string(query),
        json.array(params, parameter_to_json),
      ])
  }
}
