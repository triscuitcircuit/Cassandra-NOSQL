use std::collections::{HashMap, HashSet};
use cassandra_cpp::{BindRustType, Cluster, Session, PreparedStatement, Statement, CassResult, Batch, BatchType, Value, List, CassCollection, Set, UserType, DataType, ValueType};
use std::fs::File;
use std::io::{ self, BufRead, BufReader };
use csv::{Reader, ReaderBuilder, Trim};
use regex::Regex;
use encoding_rs::ISO_8859_10;
use encoding_rs::Encoding;
use encoding_rs_io::{DecodeReaderBytes, DecodeReaderBytesBuilder};

#[derive( Debug)]
struct Person {
    name: String,
    gender: String,
}

#[tokio::main]
async fn main() {
    use std::process;
    let session = connect().await.unwrap_or_else(|e| {
        eprintln!("Failed to connect to the database: {}", e);
        process::exit(1);
    });
    prepare_table(&session).await.unwrap_or_else(|e| {
        eprintln!("Error adding data {}", e);
        process::exit(1);
    });
    prepare_data(&session).await.unwrap_or_else(|e| {
        eprintln!("Error adding people data {}", e);
        process::exit(1);
    });


}
async fn connect() -> cassandra_cpp::Result<Session> {
    let mut cluster = Cluster::default();
    cluster.set_contact_points("127.0.0.1").unwrap();
    cluster.connect().await
}
async fn prepare_table(session: &Session)->cassandra_cpp::Result<CassResult>{
    let create_keyspace = r#"
        CREATE KEYSPACE IF NOT EXISTS moviedb
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        AND durable_writes = true;
        "#;
    let create_type = r#"
        CREATE TYPE moviedb.person (
            name text,
            gender text
        );
    "#; // User type attempt
    let create_table = r#"
    CREATE TABLE IF NOT EXISTS moviedb.movies (
        name text,
        mid int,
        year int,
        rank float,
        director text,
        cast list<frozen<set<text>>>,
        PRIMARY KEY (name, mid)
        );"#;
    let result = { // Gather results and make the Rust compiler happy
        session.execute(create_keyspace).await.unwrap();
        session.execute(create_type).await.unwrap();
        session.execute(create_table).await.unwrap()
    };
    Ok(result)
}
// Async function waits on Cassandra
// Reads data from files and loads it into the database
async fn prepare_data(session: &Session) -> cassandra_cpp::Result<PreparedStatement>{
    let movies = read_lines("IMDB/IMDBMovie.csv");

    let mut movie_map:HashMap<i32,String> = HashMap::new();

    let movie_re = Regex::new(r"^(\d+),(.*\S),(?: \((.*?)\))?(\d+),(.*?)$").unwrap();
    let prepared_statement = session.prepare(
        "INSERT INTO moviedb.movies\
            (name, mid, year, rank, director, cast)\
            VALUES (?, ?, ?, ?, ?, ?)"
    ).await.unwrap();

    let insert_cast = session.prepare(
        "UPDATE moviedb.movies SET cast = cast + ? WHERE mid = ? AND name = ?;"
    ).await.unwrap();




    let directors =  read_person_csv("IMDB/IMDBDirectors.csv")
        .records().skip(1)
        .fold(HashMap::new(), |mut acc, result|{
            if let Ok(record) = result{
                let id = record[0].to_string();
                let name = format!("{} {}",
                                   record[1].to_string(), record[2].to_string());
                acc.insert(id, name);
            }else{
                println!("{}",result.unwrap().as_slice());
            }
            acc
        });

    let person_names =  read_person_csv("IMDB/IMDBPerson.txt")
        .records().skip(1)
        .fold(HashMap::new(), |mut acc, result|{
            if let Ok(record) = result{
                let id = record[0].to_string();
                let name = format!("{} {}",
                                   record[1].to_string(), record[2].to_string());
                let gender = record[3].to_string();
                acc.insert(id, (name, gender));
            }else{
                println!("{}",result.unwrap().as_slice());
            }
            acc
        });

    let movie_directors = read_lines("IMDB/IMDBMovie_Directors.txt")
        .skip(1) // skip the header
        .map(|line| {
            let line = line.unwrap();
            let parts: Vec<&str> = line.split(',').collect();
            let did = parts[0].to_string();
            let mid = parts[1].to_string();
            (mid, did)
        })
        .collect::<Vec<_>>();

    let movie_cast = read_person_csv("IMDB/IMDBCast.txt")
        .records()
        .skip(1) // skip the header
        .filter_map(|result| {
            result.ok().map(|lines| {
                let pid = lines[0].to_string();
                let mid = lines[1].to_string();
                let role = lines[2].to_string();
                (mid, (pid, role))
            })
        })
        .collect::<Vec<_>>();

    let person_names =  read_person_csv("IMDB/IMDBPerson.txt")
        .records().skip(1)
        .fold(HashMap::new(), |mut acc, result|{
            if let Ok(record) = result{
                let id = record[0].to_string();
                let name = format!("{} {}",
                                   record[1].to_string(), record[2].to_string());
                let gender = record[3].to_string();
                acc.insert(id, (name, gender));
            }else{
                println!("{}",result.unwrap().as_slice());
            }
            acc
        });

    let default_person = ("unknown".to_string(), "unknown".to_string());



    for line in movies.skip(1){
        if let Ok(text) = line {
            let movie_re_matches =
                movie_re.captures(text.as_str()).unwrap();
            let name = movie_re_matches.get(2).unwrap().as_str();
            let mid = movie_re_matches.get(1)
                .unwrap().as_str().parse::<i32>().unwrap();
            let year = movie_re_matches.get(4).unwrap()
                .as_str().parse::<i32>().unwrap();
            let rank = movie_re_matches.get(5).unwrap()
                .as_str().parse::<f32>().unwrap_or(0.0);

            movie_map.insert(mid, name.parse().unwrap());

            let director = match movie_directors.iter().find(|(m, _)|  m.parse::<i32>() == Ok(mid)) {
                Some((_, did)) => directors.get(did).map(|name| name.to_string()),
                None => None,
            };

            let cast = movie_cast.iter()
                .filter(|(m, _)| m.parse::<i32>() == Ok(mid))
                .map(|(_, (pid, role))| {
                    let person = person_names.get(pid).map_or(
                        Person {
                            name: default_person.0.clone(),
                            gender: default_person.1.clone(),
                        },
                        |(name, gender)| Person {
                            name: name.clone(),
                            gender: gender.clone(),
                        },
                    );
                    (person, role.to_owned())
                })
                .collect::<Vec<(Person, String)>>();


            //list<set<text,text>>


            let mut final_cast = List::new();
            for (person,role) in cast {
                let mut set = Set::new();
                set.append_string(&person.name.as_str())?;
                set.append_string(role.as_str())?;
                final_cast.append_set(set)?;
            }


            // let cast = match  { }
            if director.is_some() {
                let mut statement = prepared_statement.bind();
                statement.bind(0, name)?;
                statement.bind(1, mid)?;
                statement.bind(2, year)?;
                statement.bind(3, rank)?;
                statement.bind(4, director.unwrap().as_str())?;
                statement.bind(5, final_cast)?;
                statement.execute().await?;

                }

            }

        }

    Ok(prepared_statement)
}

fn read_lines(filename: &str) -> io::Lines<BufReader<File>> {
    // Open the file in read-only mode.
    let file = File::open(filename).unwrap();
    // Read the file line by line, and return an iterator of the lines of the file.
    return BufReader::new(file).lines();
}
// fixes the encoding issue
// Rust CSV reader can only encode in UTF-8, unless a Bytesteam output is exp
fn read_person_csv(filename: &str) -> Reader<DecodeReaderBytes<File, Vec<u8>>> {
    let person_csv_file = File::open(filename);
    let transcoded_per = DecodeReaderBytesBuilder::new()
        .encoding(Some(ISO_8859_10))
        .build(person_csv_file.unwrap());
    let pdr = ReaderBuilder::new()
        .delimiter(b',')
        .from_reader(transcoded_per);
    pdr
}
