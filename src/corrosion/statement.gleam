import gleam/dict
import gleam/option

pub type Parameter {
  Null
  Bool(Bool)
  Integer(Int)
  Real(Float)
  Text(String)
  Blob(BitArray)
}

pub fn null() -> Parameter {
  Null
}

pub fn bool(value: Bool) -> Parameter {
  Bool(value)
}

pub fn integer(value: Int) -> Parameter {
  Integer(value)
}

pub fn real(value: Float) -> Parameter {
  Real(value)
}

pub fn text(value: String) -> Parameter {
  Text(value)
}

pub fn blob(value: BitArray) -> Parameter {
  Blob(value)
}

pub type Statement {
  Verbose(
    query: String,
    params: option.Option(List(Parameter)),
    named_params: option.Option(dict.Dict(String, Parameter)),
  )
  Simple(String)
  WithParams(String, List(Parameter))
  WithNamedParams(String, dict.Dict(String, Parameter))
}

pub fn verbose_statement(
  query: String,
  params: option.Option(List(Parameter)),
  named_params: option.Option(dict.Dict(String, Parameter)),
) -> Statement {
  Verbose(query, params, named_params)
}

pub fn simple_statement(query: String) -> Statement {
  Simple(query)
}

pub fn with_params(query: String, params: List(Parameter)) -> Statement {
  WithParams(query, params)
}

pub fn with_named_params(
  query: String,
  named_params: dict.Dict(String, Parameter),
) -> Statement {
  WithNamedParams(query, named_params)
}
