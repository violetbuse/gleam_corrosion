import gleam/bool
import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option
import gleam/result

pub type QueryEvent {
  Columns(column_names: List(String))
  Row(rowid: Int, values: List(dynamic.Dynamic))
  EOQ(time: Float, change_id: option.Option(Int))
  Change(
    change_type: ChangeType,
    row_id: Int,
    values: List(dynamic.Dynamic),
    change_id: Int,
  )
  QueryError(message: String)
}

pub type ChangeType {
  Insert
  Update
  Delete
}

pub fn decoder() -> decode.Decoder(QueryEvent) {
  let columns_decoder = {
    use columns <- decode.field("columns", decode.list(decode.string))
    decode.success(Columns(columns))
  }

  let row_decoder = {
    let row_tuple_decoder = {
      use rowid <- decode.field(0, decode.int)
      use rowdata <- decode.field(1, decode.list(decode.dynamic))
      decode.success(Row(rowid, rowdata))
    }

    use row <- decode.field("row", row_tuple_decoder)
    decode.success(row)
  }

  let eoq_decoder = {
    let eoq_object_decoder = {
      use time <- decode.field("time", decode.float)
      use change_id <- decode.optional_field(
        "change_id",
        option.None,
        decode.optional(decode.int),
      )
      decode.success(EOQ(time, change_id))
    }

    use eoq <- decode.field("eoq", eoq_object_decoder)
    decode.success(eoq)
  }

  let change_decoder = {
    let change_tuple_decoder = {
      let change_type_decoder = {
        use value <- decode.then(decode.string)
        case value {
          "insert" -> decode.success(Insert)
          "update" -> decode.success(Update)
          "delete" -> decode.success(Delete)
          _ -> decode.failure(Insert, "ChangeType")
        }
      }

      use change_type <- decode.field(0, change_type_decoder)
      use row_id <- decode.field(1, decode.int)
      use values <- decode.field(2, decode.list(decode.dynamic))
      use change_id <- decode.field(3, decode.int)
      decode.success(Change(change_type:, row_id:, values:, change_id:))
    }

    use change <- decode.field("change", change_tuple_decoder)
    decode.success(change)
  }

  let error_decoder = {
    use message <- decode.field("error", decode.string)
    decode.success(QueryError(message))
  }

  decode.one_of(row_decoder, [
    change_decoder,
    columns_decoder,
    eoq_decoder,
    error_decoder,
  ])
}

pub type EventHandler(datatype) {
  EventHandler(
    columns: List(String),
    rows: dict.Dict(Int, datatype),
    decoder: decode.Decoder(datatype),
    query_completed: Bool,
    change_id: option.Option(Int),
  )
}

pub fn new_event_handler(
  decoder: decode.Decoder(datatype),
) -> EventHandler(datatype) {
  EventHandler([], dict.new(), decoder, False, option.None)
}

pub fn handle_event(
  handler: EventHandler(datatype),
  event: QueryEvent,
) -> Result(EventHandler(datatype), String) {
  case event {
    Row(rowid:, values:) -> {
      let obj =
        handler.columns
        |> list.map(dynamic.string)
        |> list.zip(values)
        |> dynamic.properties
      let decode_result = decode.run(obj, handler.decoder)

      case decode_result {
        Ok(data) ->
          Ok(
            EventHandler(
              ..handler,
              rows: dict.insert(handler.rows, rowid, data),
            ),
          )
        Error(_) -> Error("Failed to decode row")
      }
    }
    Change(change_type:, row_id:, values:, change_id:) -> {
      let obj =
        handler.columns
        |> list.map(dynamic.string)
        |> list.zip(values)
        |> dynamic.properties
      let decode_result = decode.run(obj, handler.decoder)

      case decode_result {
        Error(_) -> Error("Failed to decode row")
        Ok(data) -> {
          let new_change_id = case handler.change_id, change_id {
            option.None, new_change_id -> new_change_id
            option.Some(old_change_id), new_change_id
              if old_change_id > new_change_id
            -> old_change_id
            _, new_change_id -> new_change_id
          }

          let new_dict = case change_type {
            Insert | Update -> dict.insert(handler.rows, row_id, data)
            Delete -> dict.delete(handler.rows, row_id)
          }

          Ok(
            EventHandler(
              ..handler,
              change_id: option.Some(new_change_id),
              rows: new_dict,
            ),
          )
        }
      }
    }
    Columns(column_names:) -> Ok(EventHandler(..handler, columns: column_names))
    EOQ(time: _, change_id:) ->
      Ok(EventHandler(..handler, query_completed: True, change_id:))
    QueryError(message:) -> Error(message)
  }
}

pub fn get_sorted(
  handler: EventHandler(datatype),
) -> Result(List(datatype), Nil) {
  use <- bool.guard(when: !handler.query_completed, return: Error(Nil))

  dict.to_list(handler.rows)
  |> list.sort(fn(rowa, rowb) { int.compare(rowa.0, rowb.0) })
  |> list.map(fn(row) { row.1 })
  |> Ok
}

pub fn get_unsorted(
  handler: EventHandler(datatype),
) -> Result(List(datatype), Nil) {
  use <- bool.guard(when: !handler.query_completed, return: Error(Nil))

  dict.values(handler.rows) |> Ok
}
