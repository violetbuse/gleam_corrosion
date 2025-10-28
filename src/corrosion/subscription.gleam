import corrosion/internal/util
import corrosion/query_event
import corrosion/statement
import gleam/bytes_tree
import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/http/request
import gleam/json
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/uri
import httpp/jsonl

pub type ChangeType {
  Insert
  Update
  Delete
}

fn query_event_change_type_to_event_change_type(
  in: query_event.ChangeType,
) -> ChangeType {
  case in {
    query_event.Delete -> Delete
    query_event.Insert -> Insert
    query_event.Update -> Update
  }
}

pub type Event(datatype) {
  Row(row_id: Int, data: datatype)
  Change(change_type: ChangeType, row_id: Int, data: datatype, change_id: Int)
  EndOfQuery(time: Float, change_id: option.Option(Int))
  QueryError(message: String)
  DecodeError(List(decode.DecodeError))
  Closed
}

pub type Message {
  Shutdown
}

type InternalMessage {
  ControlMessage(Message)
  StreamEvent(jsonl.JsonlEvent(query_event.QueryEvent))
}

type State(datatype) {
  State(
    decoder: decode.Decoder(datatype),
    recv: process.Subject(Event(datatype)),
    columns: List(decode.Dynamic),
    req_mgr: process.Subject(jsonl.JsonlManagerMessage),
  )
}

fn initialize(
  self: process.Subject(InternalMessage),
  corro_uri: uri.Uri,
  statement: statement.Statement,
  row_decoder: decode.Decoder(datatype),
  recv: process.Subject(Event(datatype)),
) -> Result(
  actor.Initialised(State(datatype), InternalMessage, process.Subject(Message)),
  String,
) {
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

  let jsonl_event_subject = process.new_subject()

  let req_mgr_result =
    jsonl.json_lines_stream(
      request,
      1000,
      query_event.decoder(),
      jsonl_event_subject,
    )
    |> result.replace_error("Could not start jsonl stream.")

  use #(_, req_mgr) <- result.try(req_mgr_result)

  let control_message_subject = process.new_subject()

  let initial_state =
    State(decoder: row_decoder, recv: recv, columns: [], req_mgr: req_mgr)

  let selector =
    process.new_selector()
    |> process.select(self)
    |> process.select_map(jsonl_event_subject, StreamEvent)
    |> process.select_map(control_message_subject, ControlMessage)

  actor.initialised(initial_state)
  |> actor.selecting(selector)
  |> actor.returning(control_message_subject)
  |> Ok
}

fn on_message(
  state: State(datatype),
  message: InternalMessage,
) -> actor.Next(State(datatype), InternalMessage) {
  case message {
    ControlMessage(msg) -> handle_control_message(state, msg)
    StreamEvent(evt) -> handle_jsonl_event(state, evt)
  }
}

fn handle_control_message(
  state: State(datatype),
  message: Message,
) -> actor.Next(State(datatype), InternalMessage) {
  case message {
    Shutdown -> handle_shutdown(state)
  }
}

fn handle_jsonl_event(
  state: State(datatype),
  event: jsonl.JsonlEvent(query_event.QueryEvent),
) -> actor.Next(State(datatype), InternalMessage) {
  case event {
    jsonl.Closed -> {
      process.send(state.recv, Closed)
      handle_shutdown(state)
    }
    jsonl.Line(evt) -> handle_query_event(state, evt)
  }
}

fn handle_query_event(
  state: State(datatype),
  event: query_event.QueryEvent,
) -> actor.Next(State(datatype), InternalMessage) {
  case event {
    query_event.Columns(column_names:) -> {
      actor.continue(
        State(..state, columns: list.map(column_names, dynamic.string)),
      )
    }
    query_event.EOQ(time:, change_id:) -> {
      process.send(state.recv, EndOfQuery(time:, change_id:))
      actor.continue(state)
    }
    query_event.QueryError(message:) -> {
      process.send(state.recv, QueryError(message:))
      actor.continue(state)
    }
    query_event.Row(rowid:, values:) ->
      case decode_data(state.columns, values, state.decoder) {
        Ok(data) -> {
          process.send(state.recv, Row(row_id: rowid, data:))
          actor.continue(state)
        }
        Error(event) -> {
          process.send(state.recv, event)
          actor.continue(state)
        }
      }
    query_event.Change(change_type:, row_id:, values:, change_id:) ->
      case decode_data(state.columns, values, state.decoder) {
        Ok(data) -> {
          process.send(
            state.recv,
            Change(
              change_type: query_event_change_type_to_event_change_type(
                change_type,
              ),
              row_id:,
              data:,
              change_id:,
            ),
          )
          actor.continue(state)
        }
        Error(evt) -> {
          process.send(state.recv, evt)
          actor.continue(state)
        }
      }
  }
}

fn decode_data(
  columns: List(decode.Dynamic),
  values: List(decode.Dynamic),
  decoder: decode.Decoder(datatype),
) -> Result(datatype, Event(datatype)) {
  let zipped =
    list.strict_zip(columns, values)
    |> result.map(dynamic.properties)
    |> result.replace_error(QueryError(
      "Columns array is a different length from values.",
    ))

  use dynamic <- result.try(zipped)

  let decode_result =
    decode.run(dynamic, decoder)
    |> result.try_recover(fn(decode_errors) {
      case decode.run(dynamic.list(values), decoder) {
        Ok(data) -> Ok(data)
        Error(_) -> Error(DecodeError(decode_errors))
      }
    })

  use result <- result.try(decode_result)

  Ok(result)
}

fn handle_shutdown(
  state: State(datatype),
) -> actor.Next(State(datatype), InternalMessage) {
  case process.subject_owner(state.recv) {
    Ok(pid) -> process.unlink(pid)
    _ -> Nil
  }

  process.send(state.req_mgr, jsonl.Shutdown)

  actor.stop()
}

pub fn subscribe(
  corro_uri: uri.Uri,
  statement: statement.Statement,
  row_decoder: decode.Decoder(datatype),
  recv: process.Subject(Event(datatype)),
) -> Result(process.Subject(Message), Nil) {
  let actor =
    actor.new_with_initialiser(1000, initialize(
      _,
      corro_uri,
      statement,
      row_decoder,
      recv,
    ))
    |> actor.on_message(on_message)
    |> actor.start

  case actor {
    Ok(started) -> {
      let assert True = process.is_alive(started.pid)
      process.link(started.pid)
      Ok(started.data)
    }
    _ -> Error(Nil)
  }
}

pub fn unsubscribe(subscription: process.Subject(Message)) {
  process.send(subscription, Shutdown)
}
