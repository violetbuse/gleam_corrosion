//// This module allows for decoding query events returned by /v1/queries and /v1/subscriptions

import gleam/dynamic
import gleam/dynamic/decode
import gleam/option

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
