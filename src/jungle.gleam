import gleam/io
import gleam/erlang.{start_arguments}
import glint.{CommandInput}
import glint/flag
import gleam/erlang/os
import gleam/erlang/charlist.{Charlist}
import gleam/erlang/file
import gleam/string
import gleam/option.{None, Option, Some}
import gleam/list
import gleam/pgo.{Connection}
import gleam/dynamic
import jungle_core/types.{CoreType}
import gleam/int
import gleam/regex
import gleam/map.{Map}

fn prelude() -> String {
  "import gleam/option.{Option}
import gleam/dynamic
import jungle_core
import jungle_core/types
import pgo.{Connection}

pub fn connect(url: String) -> Connection {
  assert Ok(config) = pgo.url_config(url)
  pgo.connect(config)
}\n\n"
}

external fn titlecase_(v: String) -> String =
  "string" "titlecase"

fn titlecase(v: String) -> String {
  string.split(v, "_")
  |> string.join("")
  |> protect_keywords
  |> titlecase_
}

external fn run(cmd: Charlist) -> Nil =
  "os" "cmd"

fn protect_keywords(word: String) -> String {
  case word {
    "type" -> "type_"
    "fn" -> "fn_"
    "let" -> "let_"
    "case" -> "case_"
    "assert" -> "assert_"
    "pub" -> "pub_"
    "import" -> "import_"
    "module" -> "module_"
    _ -> word
  }
}

fn run_cmd(cmd: String) -> Nil {
  run(charlist.from_string(cmd))
}

fn int_to_letter(int: Int) -> String {
  case int == 0 {
    True -> ""
    False -> {
      let remainder = { int - 1 } % 26
      let assert Ok(codepoint) = string.utf_codepoint(remainder + 97)
      let char = string.from_utf_codepoints([codepoint])
      let quotient = { int - 1 } / 26
      int_to_letter(quotient) <> char
    }
  }
}

fn save_to_file(path: String, contents: String) -> Result(Nil, String) {
  case file.write(contents, path) {
    Ok(_) -> Ok(Nil)
    Error(_) -> Error("Failed to write file")
  }
}

type QueryType {
  One
  Many
  Exec
}

type Parameter {
  Parameter(name: String, type_: CoreType)
}

type QueryInner {
  QueryInner(return_type: String, parameters: List(Parameter), sql: String)
}

type Query {
  Query(
    name: String,
    description: Option(String),
    query: QueryInner,
    type_: QueryType,
  )
}

fn get_table_name_from_query(query: String) -> Option(String) {
  let options = regex.Options(case_insensitive: False, multi_line: True)
  let assert Ok(re) = regex.compile("[from|FROM] ([a-zA-Z]+)", with: options)
  let assert sc = regex.scan(with: re, content: query)
  let assert Ok(m) = list.at(sc, 0)
  let assert Ok(name) = list.at(m.submatches, 0)
  name
}

fn parse_query_inner(
  tables_map: Map(String, Table),
  query: String,
) -> QueryInner {
  case get_table_name_from_query(query) {
    Some(tbl) -> {
      let assert Ok(table) = map.get(tables_map, tbl)
      let options = regex.Options(case_insensitive: False, multi_line: True)
      let assert Ok(re) =
        regex.compile("([a-z]+)[\\s]?[=][\\s]?(\\$[0-9]+)", with: options)
      let assert sc = regex.scan(with: re, content: query)
      let params =
        list.map(
          sc,
          fn(m) {
            let assert Ok(name_opt) = list.at(m.submatches, 0)
            let assert Some(name) = name_opt
            let assert Ok(col) = map.get(table.column_map, name)
            Parameter(name: name, type_: col.type_)
          },
        )

      QueryInner(
        return_type: titlecase(table.name),
        parameters: params,
        sql: query,
      )
    }
    None -> QueryInner(return_type: "types.Unknown", parameters: [], sql: query)
  }
}

fn parse_queries(
  tables: Map(String, Table),
  queries: List(Query),
  contents: String,
) -> Result(List(Query), String) {
  // Example Query
  // -- QueryName :one
  // -- Query Description
  // SELECT * FROM users WHERE id = $1
  // Thease should be parsed one at a time using the recursive function parse_queries(queries, remaining_contents)
  // It should expect a name, type and query but the description is optional

  case
    string.split(contents, "--")
    |> list.filter(fn(x) { !string.is_empty(x) })
    |> list.map(fn(x) { string.trim(x) })
  {
    [query, ..rest] ->
      case string.split(query, "\n") {
        [meta, sql] -> {
          let [name, type_] = string.split(meta, ":")
          let name = string.trim(name)
          case type_ {
            "one" ->
              parse_queries(
                tables,
                [
                  Query(
                    name: name,
                    description: None,
                    query: parse_query_inner(tables, sql),
                    type_: One,
                  ),
                  ..queries
                ],
                string.join(rest, "--"),
              )
            "many" ->
              parse_queries(
                tables,
                [
                  Query(
                    name: name,
                    description: None,
                    query: parse_query_inner(tables, sql),
                    type_: Many,
                  ),
                  ..queries
                ],
                string.join(rest, "--"),
              )
            "exec" ->
              parse_queries(
                tables,
                [
                  Query(
                    name: name,
                    description: None,
                    query: parse_query_inner(tables, sql),
                    type_: Exec,
                  ),
                  ..queries
                ],
                string.join(rest, "--"),
              )
            _ -> Error("Could not parse query type")
          }
        }
        _ -> Error("Could not parse query")
      }
    _ -> Ok(queries)
  }
}

fn compile(input: CommandInput) {
  let db_url = os.get_env("JUNGLE_DB")
  case db_url {
    Ok(url) -> {
      let assert Ok(flag.S(queries_path)) =
        flag.get(from: input.flags, for: "queries")
      let assert Ok(flag.S(schema)) = flag.get(from: input.flags, for: "schema")
      let assert Ok(flag.S(output_path)) =
        flag.get(from: input.flags, for: "output")
      case file.read(queries_path) {
        Ok(queries_contents) ->
          case generate_types(url, schema) {
            Ok(#(tables, table_map)) ->
              case parse_queries(table_map, [], queries_contents) {
                Ok(queries) ->
                  case compile_queries(queries, tables) {
                    Ok(out) ->
                      case save_to_file(output_path, out) {
                        Ok(_) -> {
                          run_cmd("gleam format " <> output_path)
                          io.println(
                            "Generated Schemas and written to " <> output_path,
                          )
                        }
                        Error(e) -> io.println(e)
                      }
                    Error(e) -> io.println(e)
                  }
                Error(e) -> io.println(e)
              }
            Error(_) -> io.println("Could not find tables")
          }
        // TODO handle errors
        Error(_) -> io.println("Could not read queries file")
      }
    }
    Error(_) -> io.println("No database url set")
  }
}

type Column {
  Column(name: String, type_: CoreType, nullable: Bool)
}

fn column_to_gleam(column: Column) -> String {
  protect_keywords(string.lowercase(column.name)) <> ": " <> types.to_gleam_str(
    column.type_,
    column.nullable,
  ) <> ","
}

type Table {
  Table(
    name: String,
    insertable: Bool,
    columns: List(Column),
    column_map: Map(String, Column),
  )
}

fn table_to_gleam(table: Table) -> String {
  let columns =
    list.map(table.columns, column_to_gleam)
    |> string.join("\n")

  let column_decoders =
    list.map(table.columns, fn(col) { types.decoder_str(col.type_) })
    |> string.join(",")

  let tbl =
    "pub type " <> titlecase(table.name) <> " {\n" <> titlecase(table.name) <> "(\n" <> columns <> ")\n}"
  let decoder =
    "pub fn " <> table.name <> "_decoder(tbl: dynamic.Dynamic) { tbl |> decode" <> int.to_string(list.length(
      table.columns,
    )) <> "(" <> titlecase(table.name) <> "," <> column_decoders <> ")" <> "}"
  tbl <> "\n" <> decoder
}

fn parse_columns(
  columns: List(Column),
  column_map: Map(String, Column),
  remaining: List(#(String, Int, String, String, String)),
) -> #(List(Column), Map(String, Column)) {
  case remaining {
    [column, ..rest] -> {
      let type_ = types.to_core(column.3)

      let col =
        Column(name: column.0, type_: type_, nullable: column.2 == "YES")

      parse_columns(
        [col, ..columns],
        column_map
        |> map.insert(string.lowercase(column.0), col),
        rest,
      )
    }
    _ -> #(columns, column_map)
  }
}

fn parse_table(
  schema: String,
  db: Connection,
  tables: List(Table),
  table_map: Map(String, Table),
  remaning: List(#(String, String)),
) -> Result(#(List(Table), Map(String, Table)), String) {
  case remaning {
    [table, ..rest] -> {
      let introspection_query =
        "SELECT column_name, ordinal_position, is_nullable, data_type, is_generated
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2;"

      let return_type =
        dynamic.tuple5(
          dynamic.string,
          dynamic.int,
          dynamic.string,
          dynamic.string,
          dynamic.string,
        )

      case
        pgo.execute(
          introspection_query,
          db,
          [pgo.text(schema), pgo.text(table.0)],
          return_type,
        )
      {
        Ok(response) -> {
          let #(columns, column_map) =
            parse_columns([], map.new(), response.rows)

          let tbl =
            Table(
              name: table.0,
              insertable: table.1 == "YES",
              columns: columns,
              column_map: column_map,
            )

          parse_table(
            schema,
            db,
            [tbl, ..tables],
            table_map
            |> map.insert(string.lowercase(tbl.name), tbl),
            rest,
          )
        }
        Error(_) -> Error("Failed to introspect colums for table: " <> table.0)
      }
    }

    _ -> Ok(#(tables, table_map))
  }
}

fn generate_types(
  url: String,
  schema: String,
) -> Result(#(List(Table), Map(String, Table)), String) {
  let assert Ok(db_config) = pgo.url_config(url)
  let db = pgo.connect(db_config)

  // Get list of tables
  let introspection_query =
    "SELECT table_name, is_insertable_into
FROM information_schema.tables
WHERE table_schema = $1;"

  let return_type = dynamic.tuple2(dynamic.string, dynamic.string)

  case pgo.execute(introspection_query, db, [pgo.text(schema)], return_type) {
    Ok(response) -> parse_table(schema, db, [], map.new(), response.rows)
    Error(_) -> Error("Failed to introspect tables")
  }
}

fn get_return_decoder(query_type: QueryType, query_return: String) -> String {
  case query_type {
    Exec -> ""
    Many -> "dynamic.list(" <> string.lowercase(query_return) <> "_decoder)"
    One -> string.lowercase(query_return) <> "_decoder"
  }
}

fn query_to_gleam(query: Query) -> String {
  let params_with_types =
    list.map(
      query.query.parameters,
      fn(p) {
        string.lowercase(p.name) <> ": " <> types.to_gleam_str(p.type_, False)
      },
    )
    |> string.join(",")

  let params =
    list.map(
      query.query.parameters,
      fn(p) {
        types.to_pgo_cast(p.type_) <> "(" <> string.lowercase(p.name) <> ")"
      },
    )
    |> string.join(",")

  "pub fn " <> string.lowercase(query.name) <> "(db: Connection, " <> params_with_types <> ") { \n pgo.execute(\"" <> query.query.sql <> "\", db, [" <> params <> "], " <> get_return_decoder(
    query.type_,
    query.query.return_type,
  ) <> ")\n }"
}

fn compile_queries(
  queries: List(Query),
  tables: List(Table),
) -> Result(String, String) {
  let parsed_tables = list.map(tables, table_to_gleam)

  let parsed_queries = list.map(queries, query_to_gleam)

  let decoders =
    list.map(tables, fn(tbl) { list.length(tbl.columns) })
    |> list.sort(by: int.compare)
    |> list.unique
    |> list.map(fn(size) {
      let param_list = list.range(1, size)
      let constructor_param =
        "constructor: fn(" <> string.join(
          list.map(param_list, fn(x) { "t" <> int.to_string(x) }),
          ",",
        ) <> ") -> t"

      "fn decode" <> int.to_string(size) <> "(" <> constructor_param <> "," <> string.join(
        list.map(
          param_list,
          fn(x) {
            "t" <> int.to_string(x) <> ": dynamic.Decoder(t" <> int.to_string(x) <> ")"
          },
        ),
        ",",
      ) <> ") {
        fn(x: Dynamic) {
          case " <> string.join(
        list.map(param_list, fn(x) { "t" <> int.to_string(x) <> "(x)" }),
        ",",
      ) <> " {
            " <> string.join(
        list.map(param_list, fn(x) { "Ok(" <> int_to_letter(x) <> ")" }),
        ",",
      ) <> " -> Ok(constructor(" <> string.join(
        list.map(param_list, fn(x) { int_to_letter(x) }),
        ",",
      ) <> "))
            " <> string.join(
        list.map(param_list, fn(x) { int_to_letter(x) }),
        ",",
      ) <> " -> Error(list.flatten([" <> string.join(
        list.map(
          param_list,
          fn(x) { "jungle_core.all_errors(" <> int_to_letter(x) <> ")" },
        ),
        ",",
      ) <> "]))
          }
        }
      }"
    })

  Ok(
    "// --- Prelude\n" <> prelude() <> string.join(decoders, "\n") <> "// --- Tables\n" <> string.join(
      parsed_tables,
      "\n",
    ) <> "\n// --- Queries\n" <> string.join(parsed_queries, "\n"),
  )
}

pub fn main() {
  glint.new()
  |> glint.add_command(
    at: ["compile"],
    do: compile,
    with: [
      flag.string("queries", "./queries.sql", "Query File"),
      flag.string("output", "./jungle.gleam", "Generated gleam file"),
      flag.string("schema", "public", "Postgres Schema to Introspect"),
    ],
    described: "Compiles the queries",
  )
  |> glint.run(start_arguments())
}
