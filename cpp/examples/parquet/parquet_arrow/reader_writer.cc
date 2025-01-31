// Licensed to the Apache Software Foundation (ASF) under one// or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <filesystem>

#include <fstream>
#include <iostream>
#include "arrow/ipc/writer.h"

namespace fs = std::__fs::filesystem;

// #0 Build dummy data to pass around
// To have some input data, we first create an Arrow Table that holds
// some data.
std::shared_ptr<arrow::Table> generate_table() {
  arrow::Int64Builder i64builder;
  PARQUET_THROW_NOT_OK(i64builder.AppendValues({1, 2, 3, 4, 5}));
  std::shared_ptr<arrow::Array> i64array;
  PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

  arrow::StringBuilder strbuilder;
  PARQUET_THROW_NOT_OK(strbuilder.Append("some"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("string"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("content"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("in"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("rows"));
  std::shared_ptr<arrow::Array> strarray;
  PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));

  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("int", arrow::int64()), arrow::field("str", arrow::utf8())});

  return arrow::Table::Make(schema, {i64array, strarray});
}

// #1 Write out the data as a Parquet file
void write_parquet_file(const arrow::Table& table) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("parquet-arrow-example.parquet"));
  // The last argument to the function call is the size of the RowGroup in
  // the parquet file. Normally you would choose this to be rather large but
  // for the example, we use a small value to have multiple RowGroups.
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

void write_parquet_file_to(const arrow::Table& table, const std::string& outpath) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(outpath));
  // The last argument to the function call is the size of the RowGroup in
  // the parquet file. Normally you would choose this to be rather large but
  // for the example, we use a small value to have multiple RowGroups.
  //  std::shared_ptr<parquet::WriterProperties> writer_properties =
  //      parquet::WriterProperties::Builder().compression(parquet::Compression::ZSTD)->compression_level(11)->data_pagesize(1024
  //      * 1024*16)->build();
  std::shared_ptr<parquet::WriterProperties> writer_properties =
      parquet::WriterProperties::Builder()
          .compression(parquet::Compression::UNCOMPRESSED)
          ->data_pagesize(1024 * 1024 * 16)
          ->build();

  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(),
                                                  outfile, 50 << 20, writer_properties));
}

// #2: Fully read in the file
void read_whole_file() {
  std::cout << "Reading parquet-arrow-example.parquet at once" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

std::shared_ptr<arrow::Table> read_parquet_file(const std::string& filename) {
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
      infile, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
  return table;
}

// #3: Read only a single RowGroup of the parquet file
void read_single_rowgroup() {
  std::cout << "Reading first RowGroup of parquet-arrow-example.parquet" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

// #4: Read only a single column of the whole parquet file
void read_single_column() {
  std::cout << "Reading first column of parquet-arrow-example.parquet" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->ReadColumn(0, &array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}

// #5: Read only a single column of a RowGroup (this is known as ColumnChunk)
//     from the Parquet file.
void read_single_column_chunk() {
  std::cout << "Reading first ColumnChunk of the first RowGroup of "
               "parquet-arrow-example.parquet"
            << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->Column(0)->Read(&array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}

void decompress(const std::string &inpath) {
  std::ifstream inf(inpath, std::ios::binary);
  int ncols;
  inf.read(reinterpret_cast<char *>(&ncols), sizeof(ncols));
  auto zstd = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();
  for (int i = 0; i < ncols; ++i) {
    int64_t comped_size;
    inf.read(reinterpret_cast<char *>(&comped_size), sizeof(comped_size));
    int64_t decompressed_size;
    inf.read(reinterpret_cast<char *>(&decompressed_size), sizeof(decompressed_size));

    std::vector<uint8_t> compressed(comped_size);
    inf.read(reinterpret_cast<char *>(compressed.data()), comped_size);
    std::vector<uint8_t> decompressed(decompressed_size);
    assert(zstd->Decompress(comped_size, compressed.data(), decompressed_size, decompressed.data()).ok());


  }
}

int main(int argc, char** argv) {
  //  auto ex_col_file_path =
  //  "/tmp/simweb_rewrite/part-00050-235a39c1-b238-424c-a6b3-9acc1d92076b-c000.snappy.parquet/country";
  //  std::shared_ptr<arrow::io::ReadableFile> infile;
  //  PARQUET_ASSIGN_OR_THROW(
  //      infile, arrow::io::ReadableFile::Open(ex_col_file_path,
  //      arrow::default_memory_pool()));
  //  auto reader = arrow::ipc::RecordBatchFileReader::Open(infile).ValueOrDie();
  //  auto batchread = reader->ReadRecordBatch(0).ValueOrDie();
  //  std::cout << batchread->num_rows() << std::endl;

  auto data_dir = "/Users/josephgardi/Downloads/simwebparqs/yearmonth=2022-07";
  std::string out_dir = "/tmp/simweb_rewrite";
  // create out_dir if it doesn't exist
  if (!fs::exists(out_dir)) {
    fs::create_directories(out_dir);
  }
  // benchmark on files in data_dir
  auto start_time = std::chrono::high_resolution_clock::now();
  // iterate files in data_dir
  for (auto& entry : fs::directory_iterator(data_dir)) {
    // read parquet file
    try {
      auto table = read_parquet_file(entry.path().string());
      std::cout << "read file " << entry.path().filename().string() << std::endl;
      std::string outpath = out_dir + "/" + entry.path().filename().string();
      //      auto sink = arrow::io::FileOutputStream::Open(outpath).ValueOrDie();
      std::ofstream outfile(outpath);
      auto ncols = table->num_columns();
      outfile.write(reinterpret_cast<char*>(&ncols), sizeof(ncols));

      auto zstd = arrow::util::Codec::Create(arrow::Compression::ZSTD, 15).ValueOrDie();

      for (int i = 0; i < table->num_columns(); i++) {
        auto col = table->column(i);
        auto dtype = col->type();
        auto one_col_schema = arrow::schema({table->schema()->field(i)});
        auto cols_path = outpath + "/" + one_col_schema->field(0)->name();
        auto sink = std::move(arrow::io::BufferOutputStream::Create(0)).ValueOrDie();
        auto writer = std::move(arrow::ipc::MakeFileWriter(sink.get(), one_col_schema))
                          .ValueOrDie();
        for (const auto& chunk : col->chunks()) {
//          const uint8_t zero_byte = 0;
//          if (chunk->type().get() == arrow::utf8().get()) {
//            auto utf8_chunk = std::static_pointer_cast<arrow::StringArray>(chunk);
//            //            auto *strings = chunk->data()->GetValues<std::string>(0);
//            //            auto utf8_chunk2 = std::static_pointer_cast<arrow::StringArray>(
//            //                utf8_chunk->Slice(0, utf8_chunk->length()));
//            for (int j = 0; j < utf8_chunk->length(); j++) {
//              auto str = utf8_chunk->GetString(j);
//              assert(sink->Write(str.c_str(), str.size()).ok());
//              if (j < utf8_chunk->length() - 1) {
//                assert(sink->Write(&zero_byte, 1).ok());
//              }
//            }
//            //            writer->WriteRecordBatch(*arrow::RecordBatch::Make(one_col_schema, utf8_chunk2->length(), {utf8_chunk2})).ok();
//
//          } else if (arrow::is_integer(chunk->type_id())) {
//            for (int j = 0; j < chunk->length(); j++) {
//              auto val = chunk->GetScalar(j).ValueOrDie()->ToString();
//              if (chunk->IsNull(j)) {
//                std::cout << "got null val " << val << " at " << j << std::endl;
//              }
//              assert(sink->Write(val.c_str(), val.size()).ok());
//              if (j < chunk->length() - 1) {
//                assert(sink->Write(&zero_byte, 1).ok());
//              }
//            }
//          } else {
            auto batch = arrow::RecordBatch::Make(std::move(one_col_schema),
                                                  chunk->length(), {chunk});
            assert(writer->WriteRecordBatch(*batch).ok());
//          }
        }
        assert(writer->Close().ok());
        auto buf = sink->Finish().ValueOrDie();
        std::cout << "got buf "  << buf->size() << std::endl;
        int64_t zstdMaxLen = zstd->MaxCompressedLen(buf->size(), buf->data());
        std::vector<uint8_t> zstdBuf(zstdMaxLen);
        int64_t orig_size = buf->size();
        int64_t comped_size =
            zstd->Compress(buf->size(), buf->data(), zstdMaxLen, zstdBuf.data()).ValueOrDie();
        outfile.write(reinterpret_cast<const char*>(&comped_size), sizeof(comped_size));
        outfile.write(reinterpret_cast<const char*>(&orig_size), sizeof(orig_size));
        outfile.write(reinterpret_cast<char*>(zstdBuf.data()), comped_size);
        //        std::cout << "got buffer of size " << buf->size() << std::endl;
        //        auto one_col_table = arrow::Table::Make(one_col_schema, {col},
        //        table->num_rows()); write_parquet_file_to(*one_col_table, cols_dir);
        //        auto onearr = col->chunk(0);
      }
      // write parquet file
      //      write_parquet_file_to(*table, outpath);
      //      std::cout << "wrote file" << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Error: " << e.what() << std::endl;
    }
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  std::cout << "Time taken: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                     start_time)
                   .count()
            << "ms" << std::endl;
  //  auto table =
  //  read_parquet_file("/Users/josephgardi/Downloads/ooklasample/2019-01-01_performance_mobile_tiles.parquet");
  //  write_parquet_file_to(*table, "/tmp/out.parquet");
  return 0;
  //  std::shared_ptr<arrow::Table> table = generate_table();
  //  write_parquet_file(*table);
  //  read_whole_file();
  //  read_single_rowgroup();
  //  read_single_column();
  //  read_single_column_chunk();
}
