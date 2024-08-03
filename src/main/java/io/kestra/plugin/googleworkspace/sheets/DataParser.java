package io.kestra.plugin.googleworkspace.sheets;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class DataParser {

	private final RunContext runContext;

	public List<List<Object>> parseCsv(File file, AbstractLoad.CsvOptions csvOptions) throws IOException, IllegalVariableEvaluationException {
		List<List<Object>> result = new ArrayList<>();

		Charset charset = Charset.forName(
			runContext.render(csvOptions.getEncoding())
		);

		FileReader reader = new FileReader(file, charset);
		CSVFormat format = getCsvFormat(csvOptions);
		try (CSVParser parser = new CSVParser(reader, format)) {
			for (CSVRecord record : parser) {
				List<Object> row = new ArrayList<>();

				record.forEach(row::add);
				result.add(row);
			}
			return result;
		}
	}

	public List<List<Object>> parseThroughMapper(File file, ObjectMapper mapper, boolean includeHeaders) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		List<Map<String, Object>> list = mapper.readValue(file, List.class);

		if (includeHeaders) {
			List<ArrayList<Object>> headers = list.stream()
				.map(Map::keySet)
				.map(column -> new ArrayList<Object>(column))
				.distinct()
				.toList();

			result.addAll(headers);
		}

		result.addAll(
			list.stream()
				.map(Map::values)
				.map(ArrayList::new)
				.toList()
		);

		return result;
	}

	public List<List<Object>> parseAvro(File file, boolean includeHeaders, String avroSchema) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		boolean isHeaderIncluded = false;
		try (org.apache.avro.file.FileReader<GenericRecord> reader =
			     DataFileReader.openReader(file, new GenericDatumReader<>())
		) {
			Schema schema;
			if (avroSchema != null) {
				schema = new Schema.Parser().parse(avroSchema);
			} else {
				schema = reader.getSchema();
			}

			while (reader.hasNext()) {
				GenericRecord record = reader.next();
				List<Object> row = new ArrayList<>();

				if (includeHeaders && !isHeaderIncluded) {
					List<Object> headers = new ArrayList<>(
						schema.getFields()
							.stream()
							.map(Schema.Field::name)
							.toList()
					);

					isHeaderIncluded = true;
					result.add(headers);
				}

				schema.getFields()
					.forEach(field -> row.add(record.get(field.name()).toString()));

				result.add(row);
			}
		}

		return result;
	}

	public List<List<Object>> parseParquet(File file, boolean includeHeaders) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		boolean isHeaderIncluded = false;
		Configuration configuration = new Configuration();
		LocalInputFile inputFile = new LocalInputFile(file.toPath());
		try (ParquetReader<GenericRecord> reader = AvroParquetReader
			.<GenericRecord>builder(inputFile).withConf(configuration).build()
		) {
			GenericRecord record;
			while ((record = reader.read())!= null) {
				List<Object> row = new ArrayList<>();

				if (includeHeaders && !isHeaderIncluded) {
					List<Object> headers = new ArrayList<Object>(
						record.getSchema()
							.getFields()
							.stream()
							.map(Schema.Field::name)
							.toList()
					);

					isHeaderIncluded = true;
					result.add(headers);
				}

				GenericRecord finalRecord = record;
				record.getSchema()
					.getFields()
					.forEach(field -> row.add(
						finalRecord.get(field.name()).toString())
					);

				result.add(row);
			}
		}

		return result;
	}

	public List<List<Object>> parseORC(File file, boolean includeHeaders) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		Configuration configuration = new Configuration();
		Path path = new Path(file.getPath());
		try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(configuration))) {
			TypeDescription schema = reader.getSchema();
			VectorizedRowBatch rowBatch = schema.createRowBatch();

			try (RecordReader rows = reader.rows()) {
				if (includeHeaders) {
					result.add(
						new ArrayList<Object>(
							schema.getFieldNames()
						)
					);
				}

				while (rows.nextBatch(rowBatch)) {
					for (int row = 0; row < rowBatch.size; row++) {
						List<Object> record = new ArrayList<>();

						for (ColumnVector vector : rowBatch.cols) {
							record.add(
								getValue(vector, row)
							);
						}

						result.add(record);
					}
				}
			}
		}

		return result;
	}

	private Object getValue(ColumnVector vector, int row) {
		return switch (vector) {
			case LongColumnVector longVector -> {
				if (vector instanceof DateColumnVector dateVector) {
					yield dateVector.formatDate(row);
				} else {
					yield longVector.vector[row];
				}
			}
			case DoubleColumnVector doubleVector -> doubleVector.vector[row];
			case BytesColumnVector bytesVector -> {
				if (!bytesVector.isNull[row]) {
					int start = bytesVector.start[row];
					int length = bytesVector.length[row];
					yield new String(bytesVector.vector[row], start, length);
				} else {
					yield "";
				}
			}
			case DecimalColumnVector decimalVector -> decimalVector.vector[row];
			case TimestampColumnVector timeVector -> timeVector.time[row];
			case null, default -> "Unsupported type";
		};
	}

	private CSVFormat getCsvFormat(AbstractLoad.CsvOptions csvOptions) throws IllegalVariableEvaluationException {
		return CSVFormat.Builder.create()
			.setDelimiter(
				csvOptions.getFieldDelimiter() != null ?
					runContext.render(csvOptions.getFieldDelimiter()) :
					CSVFormat.DEFAULT.getDelimiterString()
			)
			.setQuote(
				csvOptions.getQuote() != null ?
					runContext.render(csvOptions.getQuote()).charAt(0) :
					CSVFormat.DEFAULT.getQuoteCharacter()
			)
			.setRecordSeparator(CSVFormat.DEFAULT.getRecordSeparator())
			.setIgnoreEmptyLines(true)
			.setAllowDuplicateHeaderNames(false)
			.setSkipHeaderRecord(csvOptions.getSkipLeadingRows() != null && csvOptions.getSkipLeadingRows() > 0)
			.build();
	}

}
