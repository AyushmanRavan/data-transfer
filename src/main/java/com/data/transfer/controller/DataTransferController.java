package com.data.transfer.controller;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.data.transfer.request.ConnectionRequestDetails;
import com.data.transfer.request.DataBaseTransferRequestDetails;
import com.data.transfer.response.ConnectionStatusResponse;
import com.data.transfer.utils.ConnectionUtils;

@RestController
@RequestMapping("/resources")
public class DataTransferController {

	@Autowired
	private ConnectionUtils connectionUtils;

	@PostMapping(path = "/check-connection-status")
	public ResponseEntity<?> checkDataBaseConnectionStatus(
			@RequestBody ConnectionRequestDetails connectionRequestDetails) {
		return new ResponseEntity<>(connectionUtils.checkDataBaseConnectionStatus(connectionRequestDetails),
				HttpStatus.OK);
	}

	@SuppressWarnings("unused")
	@PostMapping(path = "/transfer-data-base")
	public ResponseEntity<?> transferDataBase(@RequestBody DataBaseTransferRequestDetails dataBaseDetailsRequest) {

		List<ConnectionStatusResponse> response_list = new ArrayList<ConnectionStatusResponse>();
		Connection source_connection = null;
		ResultSet meta_data_result_set = null;
		try {
			source_connection = connectionUtils.getDataBaseConnection(dataBaseDetailsRequest.getSourceDriverClassName(),
					dataBaseDetailsRequest.getSourceConnectionURL(), dataBaseDetailsRequest.getSourceUserName(),
					dataBaseDetailsRequest.getSourcePassword());
			DatabaseMetaData database_meta_data = source_connection.getMetaData();
			meta_data_result_set = database_meta_data.getTables(null, null, null, new String[] { "TABLE" });

			long start = System.currentTimeMillis();
			while (meta_data_result_set.next()) {

				dataBaseDetailsRequest.setSourceTableName(meta_data_result_set.getString("TABLE_NAME"));
				dataBaseDetailsRequest.setDestinationTableName(meta_data_result_set.getString("TABLE_NAME"));

				Statement source_statement = source_connection.createStatement();
				ResultSet source_result_data = source_statement
						.executeQuery(connectionUtils.getMetaDataQuery(dataBaseDetailsRequest));
				ResultSetMetaData source_result_set_meta_data = source_result_data.getMetaData();
				int total_column_count = source_result_set_meta_data.getColumnCount();

				List<String> columns_name_list = new ArrayList<String>();
				for (int i = 1; i <= total_column_count; i++) {
					columns_name_list.add(source_result_set_meta_data.getColumnName(i));
				}

				String createTableQuery = connectionUtils.createTableQuery(source_result_data,
						source_result_set_meta_data, dataBaseDetailsRequest);

				// it should return status of created or not created table.
				ConnectionStatusResponse table_create_status = connectionUtils.executeCreateTableQuery(createTableQuery,
						dataBaseDetailsRequest);

				if (table_create_status.isStatus()) {

					int total_row_count = connectionUtils.getTotalRowCount(dataBaseDetailsRequest);
					int fixed_fetch_offset = connectionUtils.getOffset(total_column_count, total_row_count);

					if (total_row_count > 0) {
						String destination_insert_query = connectionUtils
								.generateInsertQuery(source_result_set_meta_data, dataBaseDetailsRequest);
						int offset_count = 0;

						while (total_row_count > offset_count) {
							String SELECT_PEGINATION_QUERY = "SELECT * FROM ["
									+ dataBaseDetailsRequest.getSourceDataBase() + "].[dbo].["
									+ dataBaseDetailsRequest.getSourceTableName() + "] ORDER BY "
									+ columns_name_list.get(0) + " OFFSET " + offset_count + " ROWS FETCH NEXT "
									+ fixed_fetch_offset + " ROWS ONLY";

							connectionUtils.selectDataFromSourceTableAndInsertDataIntoDestinationTable(
									SELECT_PEGINATION_QUERY, dataBaseDetailsRequest, destination_insert_query,
									columns_name_list, fixed_fetch_offset);

							offset_count = offset_count + fixed_fetch_offset;
						}

					} else {
						// table does not have data.
					}

				} else {
					// failed table creation.
					response_list.add(table_create_status);
				}

			}

			if (response_list != null && !response_list.isEmpty())
				return new ResponseEntity<>(response_list, HttpStatus.OK);
			else
				return new ResponseEntity<>(
						new ConnectionStatusResponse(true, "DataBase Successfuly migrated. Time taken in millis = "
								+ (System.currentTimeMillis() - start)),
						HttpStatus.OK);
		} catch (ClassNotFoundException ex) {
			return new ResponseEntity<>(new ConnectionStatusResponse(false, "Incorrect driver class name details."),
					HttpStatus.OK);
		} catch (SQLException ex) {
			return new ResponseEntity<>(new ConnectionStatusResponse(false, ex.getLocalizedMessage()), HttpStatus.OK);
		}

	}

	@PostMapping(path = "/transfer-specific-table")
	public ResponseEntity<?> transferSpecificTable(@RequestBody DataBaseTransferRequestDetails dataBaseDetailsRequest) {
		int total_column_count = 0;
		Connection source_connection = null;
		Statement source_statement = null;
		ResultSet source_result_data = null;
		ConnectionStatusResponse table_create_status = null;
		try {

			source_connection = connectionUtils.getDataBaseConnection(dataBaseDetailsRequest.getSourceDriverClassName(),
					dataBaseDetailsRequest.getSourceConnectionURL(), dataBaseDetailsRequest.getSourceUserName(),
					dataBaseDetailsRequest.getSourcePassword());

			source_statement = source_connection.createStatement();
			source_result_data = source_statement
					.executeQuery(connectionUtils.getMetaDataQuery(dataBaseDetailsRequest));

			ResultSetMetaData result_set_meta_data = source_result_data.getMetaData();
			total_column_count = result_set_meta_data.getColumnCount();

			List<String> columns_name_list = new ArrayList<String>();
			for (int i = 1; i <= total_column_count; i++) {
				columns_name_list.add(result_set_meta_data.getColumnName(i));
			}

			String createTableQuery = connectionUtils.createTableQuery(source_result_data, result_set_meta_data,
					dataBaseDetailsRequest);

			// it should return status of created or not created table.
			table_create_status = connectionUtils.executeCreateTableQuery(createTableQuery, dataBaseDetailsRequest);

			if (table_create_status.isStatus()) {
				int total_row_count = connectionUtils.getTotalRowCount(dataBaseDetailsRequest);
				int fixed_fetch_offset = connectionUtils.getOffset(total_column_count, total_row_count);
				if (total_row_count > 0) {

					String destination_insert_query = connectionUtils.generateInsertQuery(result_set_meta_data,
							dataBaseDetailsRequest);

					int offset_count = 0;
					long start = System.currentTimeMillis();
					while (total_row_count > offset_count) {
						String SELECT_PEGINATION_QUERY = "SELECT * FROM [" + dataBaseDetailsRequest.getSourceDataBase()
								+ "].[dbo].[" + dataBaseDetailsRequest.getSourceTableName() + "] ORDER BY "
								+ columns_name_list.get(0) + " OFFSET " + offset_count + " ROWS FETCH NEXT "
								+ fixed_fetch_offset + " ROWS ONLY";

//						ResultSet selectQueryResultSet = connectionUtils
//								.selectDataFromSourceTable(SELECT_PEGINATION_QUERY, dataBaseDetailsRequest);

//						connectionUtils.insertDataInToDestinationTable(destination_insert_query, columns_name_list,
//								selectQueryResultSet, dataBaseDetailsRequest,fixed_fetch_offset);

						connectionUtils.selectDataFromSourceTableAndInsertDataIntoDestinationTable(
								SELECT_PEGINATION_QUERY, dataBaseDetailsRequest, destination_insert_query,
								columns_name_list, fixed_fetch_offset);

						offset_count = offset_count + fixed_fetch_offset;
					}

					return new ResponseEntity<>(new ConnectionStatusResponse(true,
							"Time taken in millis = " + (System.currentTimeMillis() - start)), HttpStatus.OK);
				} else {
					return new ResponseEntity<>(new ConnectionStatusResponse(true, "Table does not have data."),
							HttpStatus.OK);
				}
			}

		} catch (ClassNotFoundException | SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (source_result_data != null)
					source_result_data.close();// close resultSet
				if (source_statement != null)
					source_statement.close(); // close PreparedStatement
				if (source_connection != null)
					source_connection.close(); // close connection
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}

		return new ResponseEntity<>(table_create_status, HttpStatus.OK);
	}

	/*
	 * System.out.println(resultSetMetaData.isNullable(i) + "" +
	 * resultSetMetaData.getColumnName(i) + " " +
	 * resultSetMetaData.isAutoIncrement(i) + " " +
	 * resultSetMetaData.getColumnTypeName(i));
	 */

//	finally{
//        try {
//              if(rs!=null) rs.close(); //close resultSet
//              if(prepStmt!=null) prepStmt.close(); //close PreparedStatement
//              if(con!=null) con.close(); // close connection
//        } catch (SQLException e) {
//              e.printStackTrace();
//        }
//  }
}
