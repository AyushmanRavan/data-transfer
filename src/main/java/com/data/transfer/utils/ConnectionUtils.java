package com.data.transfer.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.data.transfer.request.ConnectionRequestDetails;
import com.data.transfer.request.DataBaseTransferRequestDetails;
import com.data.transfer.response.ConnectionStatusResponse;

@Component
public class ConnectionUtils {

	public ConnectionStatusResponse checkDataBaseConnectionStatus(ConnectionRequestDetails connectionRequestDetails) {

		String exceptionMessage = "Connection established successfully with database.";
		Connection connection = null;
		try {
			connection = getDataBaseConnection(connectionRequestDetails.getDriverClassName(),
					connectionRequestDetails.getConnectionURL(), connectionRequestDetails.getUserName(),
					connectionRequestDetails.getPassword());
		} catch (ClassNotFoundException ex) {
			exceptionMessage = "Incorrect driver class name details.";
		} catch (SQLException ex) {
			exceptionMessage = ex.getLocalizedMessage();
		}
		if (null == connection)
			return new ConnectionStatusResponse(false, exceptionMessage);
		else
			return new ConnectionStatusResponse(true, exceptionMessage);

	}

	public Connection getDataBaseConnection(String driverClassName, String connectionURL, String userName,
			String password) throws ClassNotFoundException, SQLException {

		// registering MsSql driver class.
		Class.forName(driverClassName);

		// getting connection.
		return DriverManager.getConnection(connectionURL, userName, password);

	}

	public int getOffset(int totalColumnCount, int totalRowCount) {

//		int offset = 0;
//		if (totalRowCount >= 100000) {
//			offset = 5000;
//		}
//		else if (totalRowCount >= 10000) {
//			offset = 500;
//		}
//		else if (totalRowCount >= 1000) {
//			offset = 250;
//		} else {
//			offset = 125;
//		}

		return 35000;
	}

	public int getTotalRowCount(DataBaseTransferRequestDetails dataBaseDetailsRequest) {
		Connection sourceConnection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		String SQL_COUNT_QUERY = "SELECT COUNT(*) as total_count FROM [" + dataBaseDetailsRequest.getSourceDataBase()
				+ "].[dbo].[" + dataBaseDetailsRequest.getSourceTableName() + "]";
		try {
			sourceConnection = getDataBaseConnection(dataBaseDetailsRequest.getSourceDriverClassName(),
					dataBaseDetailsRequest.getSourceConnectionURL(), dataBaseDetailsRequest.getSourceUserName(),
					dataBaseDetailsRequest.getSourcePassword());

			statement = sourceConnection.createStatement();
			resultSet = statement.executeQuery(SQL_COUNT_QUERY);

			while (resultSet.next()) {
				return resultSet.getInt("total_count");
			}

		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (statement != null)
					statement.close(); // close PreparedStatement
				if (sourceConnection != null)
					sourceConnection.close(); // close connection
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	public ResultSet selectDataFromSourceTable(String pegination_select_query,
			DataBaseTransferRequestDetails dataBaseDetailsRequest) {
		Connection sourceConnection = null;
		PreparedStatement sourcePreparedStatement = null;
		ResultSet sourceResult = null;

		// start source related Activities.
		try {
			sourceConnection = getDataBaseConnection(dataBaseDetailsRequest.getSourceDriverClassName(),
					dataBaseDetailsRequest.getSourceConnectionURL(), dataBaseDetailsRequest.getSourceUserName(),
					dataBaseDetailsRequest.getSourcePassword());
			sourcePreparedStatement = sourceConnection.prepareStatement(pegination_select_query);
			sourceResult = sourcePreparedStatement.executeQuery();
		} catch (ClassNotFoundException | SQLException ex) {
			ex.printStackTrace();
			return sourceResult;
		} finally {
			try {
				if (sourcePreparedStatement != null)
					sourcePreparedStatement.close(); // close PreparedStatement
				if (sourceConnection != null)
					sourceConnection.close(); // close connection
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}

		return sourceResult;
	}

	public void selectDataFromSourceTableAndInsertDataIntoDestinationTable(String pegination_select_query,
			DataBaseTransferRequestDetails dataBaseDetailsRequest, String destination_insert_query,
			List<String> columns_name_list, int fixed_fetch_offset) {
		Connection sourceConnection = null;
		Connection destinationConnection = null;
		PreparedStatement sourcePreparedStatement = null;
		PreparedStatement destinationPreparedStatement = null;
		ResultSet sourceResult = null;
		int batchCounnter = 0;
		try {

			// start source related Activities.
			sourceConnection = getDataBaseConnection(dataBaseDetailsRequest.getSourceDriverClassName(),
					dataBaseDetailsRequest.getSourceConnectionURL(), dataBaseDetailsRequest.getSourceUserName(),
					dataBaseDetailsRequest.getSourcePassword());
			sourcePreparedStatement = sourceConnection.prepareStatement(pegination_select_query);
			sourceResult = sourcePreparedStatement.executeQuery();
			System.out.println(pegination_select_query);
			// start destination related Activities.
			destinationConnection = getDataBaseConnection(dataBaseDetailsRequest.getDestinationDriverClassName(),
					dataBaseDetailsRequest.getDestinationConnectionURL(),
					dataBaseDetailsRequest.getDestinationUserName(), dataBaseDetailsRequest.getDestinationPassword());

			destinationPreparedStatement = destinationConnection.prepareStatement(destination_insert_query);

			while (sourceResult.next()) {
				int preparedStatementCounter = 1;
				for (String columnName : columns_name_list) {
					destinationPreparedStatement.setString(preparedStatementCounter,
							sourceResult.getString(columnName));
					preparedStatementCounter++;
				}
				destinationPreparedStatement.addBatch();
				if (batchCounnter % fixed_fetch_offset == 0)
					destinationPreparedStatement.executeBatch();

				batchCounnter++;
			}
			destinationPreparedStatement.executeBatch();
		} catch (ClassNotFoundException | SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (sourceResult != null)
					sourceResult.close(); // close resultSet
				if (sourcePreparedStatement != null)
					sourcePreparedStatement.close(); // close PreparedStatement
				if (sourceConnection != null)
					sourceConnection.close(); // close connection
				if (destinationPreparedStatement != null)
					destinationPreparedStatement.close(); // close PreparedStatement
				if (destinationConnection != null)
					destinationConnection.close(); // close connection
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void insertDataInToDestinationTable(String insert_query, List<String> columns_list, ResultSet sourceResult,
			DataBaseTransferRequestDetails dataBaseDetailsRequest, int fixed_fetch_offset) {

		Connection destinationConnection = null;
		PreparedStatement preparedStatement = null;
		int BATCH_SIZE = fixed_fetch_offset;
		int batchCounnter = 0;

		try {
			destinationConnection = getDataBaseConnection(dataBaseDetailsRequest.getDestinationDriverClassName(),
					dataBaseDetailsRequest.getDestinationConnectionURL(),
					dataBaseDetailsRequest.getDestinationUserName(), dataBaseDetailsRequest.getDestinationPassword());

			preparedStatement = destinationConnection.prepareStatement(insert_query);

			while (sourceResult.next()) {
				int indexCounter = 1;
				for (String columnName : columns_list) {
					preparedStatement.setString(indexCounter, sourceResult.getString(columnName));
					indexCounter++;
				}
				preparedStatement.addBatch();

				if (batchCounnter % BATCH_SIZE == 0)
					preparedStatement.executeBatch();

				batchCounnter++;
			}
			preparedStatement.executeBatch();
		} catch (ClassNotFoundException | SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.close(); // close PreparedStatement
				if (destinationConnection != null)
					destinationConnection.close(); // close connection
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public String getMetaDataQuery(DataBaseTransferRequestDetails dataBaseDetailsRequest) {
		return "SELECT TOP 1 * FROM [" + dataBaseDetailsRequest.getSourceDataBase() + "].[dbo].["
				+ dataBaseDetailsRequest.getSourceTableName() + "]";
	}

//	public ResultSet getColumnsDetails(DataBaseTransferRequestDetails dataBaseDetailsRequest) {
//		Connection sourceConnection = null;
//		ResultSet sourceResult = null;
//		Statement statement = null;
//		String SQL_METADATA_QUERY = getMetaDataQuery(dataBaseDetailsRequest);
//		System.out.println("meta data purpose select query " + SQL_METADATA_QUERY);
//		try {
//			sourceConnection = getDataBaseConnection(dataBaseDetailsRequest.getSourceDriverClassName(),
//					dataBaseDetailsRequest.getSourceConnectionURL(), dataBaseDetailsRequest.getSourceUserName(),
//					dataBaseDetailsRequest.getSourcePassword());
//
//			statement = sourceConnection.createStatement();
//			sourceResult = statement.executeQuery(SQL_METADATA_QUERY);
//
//			System.out.println("success");
//			return sourceResult;
//		} catch (ClassNotFoundException | SQLException ex) {
//			ex.printStackTrace();
//			System.out.println("SQL_METADATA_QUERY exception");
//		} finally {
//			try {
//				if (statement != null)
//					statement.close(); // close PreparedStatement
//				if (sourceConnection != null)
//					sourceConnection.close(); // close connection
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
//		System.out.println("fail");
//		return sourceResult;
//	}

	public ConnectionStatusResponse executeCreateTableQuery(String createTableQuery,
			DataBaseTransferRequestDetails dataBaseDetailsRequest) {
		Connection destinationConnection = null;
		PreparedStatement prepStmt = null;
		String exceptionMessage = null;
		boolean flag = false;
		try {
			destinationConnection = getDataBaseConnection(dataBaseDetailsRequest.getDestinationDriverClassName(),
					dataBaseDetailsRequest.getDestinationConnectionURL(),
					dataBaseDetailsRequest.getDestinationUserName(), dataBaseDetailsRequest.getDestinationPassword());
			prepStmt = destinationConnection.prepareStatement(createTableQuery);
			// execute CREATE table query
			prepStmt.executeUpdate();
			flag = true;
		} catch (ClassNotFoundException | SQLException ex) {
			exceptionMessage = ex.getLocalizedMessage() + createTableQuery;
		} finally {
			try {
				if (prepStmt != null)
					prepStmt.close(); // close PreparedStatement
				if (destinationConnection != null)
					destinationConnection.close(); // close connection
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		if (flag)
			return new ConnectionStatusResponse(flag, "Successfully created table.");
		else
			return new ConnectionStatusResponse(flag, exceptionMessage);

	}

	public String createTableQuery(ResultSet sourceResult, ResultSetMetaData resultSetMetaData,
			DataBaseTransferRequestDetails dataBaseDetailsRequest) {
		String CREATE_TABLE_QUERY = "CREATE TABLE [" + dataBaseDetailsRequest.getDestinationDataBase() + "].[dbo].["
				+ dataBaseDetailsRequest.getDestinationTableName() + "](";
		try {
			int columnCount = resultSetMetaData.getColumnCount();

			for (int i = 1; i <= columnCount; i++) {

//				System.out.println(resultSetMetaData.isNullable(i) + "" + resultSetMetaData.getColumnName(i) + " "
//						+ resultSetMetaData.isAutoIncrement(i) + " " + resultSetMetaData.getColumnTypeName(i) + "  "
//						+ resultSetMetaData.getColumnDisplaySize(i));

				CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + resultSetMetaData.getColumnName(i) + " "
						+ resultSetMetaData.getColumnTypeName(i);

				switch (resultSetMetaData.getColumnTypeName(i)) {

				case "varchar":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				case "nvarchar":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				case "numeric":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(19, 0)";
					break;

				case "datetime2":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(7)";
//					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				case "binary":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				case "char":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				case "datetimeoffset":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(7)";
					break;

				case "decimal":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				case "nchar":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				case "time":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(7)";
					break;

				case "varbinary":
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + "(" + resultSetMetaData.getColumnDisplaySize(i) + ")";
					break;

				}

				if (resultSetMetaData.isAutoIncrement(i)) {
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + " IDENTITY(1,1) PRIMARY KEY ";
				}

				int columnNoNulls = resultSetMetaData.isNullable(i);
				if (columnNoNulls == 0) {
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + " NOT NULL, ";
				} else {
					CREATE_TABLE_QUERY = CREATE_TABLE_QUERY + " NULL, ";
				}

			}
			CREATE_TABLE_QUERY = CREATE_TABLE_QUERY.substring(0, CREATE_TABLE_QUERY.length() - 2) + ")";

		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		return CREATE_TABLE_QUERY;

	}

	public String generateInsertQuery(ResultSetMetaData resultSetMetaData,
			DataBaseTransferRequestDetails dataBaseDetailsRequest) {
		List<String> columnsList = new ArrayList<String>();
		int columnCount;
		try {
			columnCount = resultSetMetaData.getColumnCount();

			for (int i = 1; i <= columnCount; i++) {

				if (!resultSetMetaData.isAutoIncrement(i))
					columnsList.add(resultSetMetaData.getColumnName(i));

			}

		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		return insertQuery(dataBaseDetailsRequest.getDestinationDataBase(),
				dataBaseDetailsRequest.getDestinationTableName(), columnsList);
	}

	private String insertQuery(String newDataBaseName, String newTableName, List<String> columns) {

		String INSERT_QUERY = "INSERT INTO [" + newDataBaseName + "].[dbo].[" + newTableName + "] ( ";
		String questionMarks = " ( ";

		for (String column_name : columns) {
			INSERT_QUERY = INSERT_QUERY + column_name + ", ";
			questionMarks = questionMarks + "?, ";
		}

		INSERT_QUERY = INSERT_QUERY.substring(0, INSERT_QUERY.length() - 2) + ")";
		questionMarks = questionMarks.substring(0, questionMarks.length() - 2) + ")";

		return INSERT_QUERY + " VALUES " + questionMarks;

	}
}
