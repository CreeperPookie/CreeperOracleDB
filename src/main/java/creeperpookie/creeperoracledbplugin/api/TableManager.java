package creeperpookie.creeperoracledbplugin.api;

import com.google.gson.JsonElement;
import creeperpookie.creeperoracledbplugin.CreeperOracleDBPlugin;
import creeperpookie.creeperoracledbplugin.util.Utility;
import oracle.jdbc.driver.OracleDriver;
import oracle.security.pki.OraclePKIProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.Security;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TableManager extends Thread
{
	private static final ArrayList<Connection> CONNECTIONS = new ArrayList<>();
	private Connection connection;
	private final String tnsName;
	private final String username;
	private final String password;
	private final String table;
	//private final OracleDatabase database;
	//private final OracleRDBMSClient client;

	public TableManager(String tnsName, String username, String password, String table)
	{
		super("TableManager-" + table);
		this.tnsName = tnsName;
		this.username = username;
		this.password = password;
		this.table = table;
		this.start();
		//client = new OracleRDBMSClient();
		//database = client.getDatabase(connection);
	}

	@Override
	public void run()
	{
		Properties properties = new Properties();
		properties.setProperty("user", username);
		properties.setProperty("password", password);
		properties.setProperty("javax.net.ssl.trustStore", "C:\\Users\\CreeperPookie\\Downloads\\Wallet_MinecraftData\\cwallet.sso");
		properties.setProperty("javax.net.ssl.trustStoreType","SSO");
		properties.setProperty("javax.net.ssl.keyStore","C:\\Users\\CreeperPookie\\Downloads\\Wallet_MinecraftData\\cwallet.sso");
		properties.setProperty("javax.net.ssl.keyStoreType","SSO");
		properties.setProperty("oracle.net.authentication_services","(TCPS)");
		try
		{
			DriverManager.registerDriver(new OracleDriver());
			Security.addProvider(new OraclePKIProvider());
			connection = DriverManager.getConnection("jdbc:oracle:thin:@" + tnsName, properties);
			CONNECTIONS.add(connection);
			CreeperOracleDBPlugin.getInstance().getLogger().info("Connected to database table: " + table);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets if this TableManage is ready for database operations
	 *
	 * @return true if the connection is not null and not closed, false otherwise
	 */
	public boolean isReady()
	{
		try
		{
			return connection != null && !connection.isClosed();
		}
		catch (SQLException e)
		{
			return false;
		}
	}

	/**
	 * Gets the table name.
	 *
	 * @return the name of the table
	 */
	public String getTable()
	{
		return table;
	}

	/**
	 * Creates a new TableManager instance for a different table name using the same connection details.
	 *
	 * @param table the name of the table to get an instance of
	 * @return a new TableManager instance representing the specified table
	 * @throws SQLException if an error occurs while reconnecting to the database or creating the new instance
	 */
	public TableManager getNewTable(String table) throws SQLException
	{
		return new TableManager(tnsName, username, password, table);
	}

	/**
	 * Checks if the table exists in the database.
	 *
	 * @return true if the table exists, false otherwise
	 */
	public boolean exists()
	{
		try (ResultSet resultSet = connection.getMetaData().getTables(null, null, table.toUpperCase(), null))
		{
			return resultSet.next();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Checks if the table is empty.
	 *
	 * @return true if the table is empty, false otherwise
	 */
	public boolean isEmpty()
	{
		try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + table))
		{
			if (resultSet.next())
			{
				return resultSet.getInt(1) == 0;
			}
			else
			{
				throw new IllegalStateException("Failed to check if table is empty");
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Creates a table in the database.
	 * If the table already exists, an IllegalArgumentException is thrown.
	 *
	 * @param columnNames the columns to create
	 * @param columnDataTypes the data types of the columns, defined in the same order as the column names
	 */
	public void createTable(List<String> columnNames, List<Class<?>> columnDataTypes)
	{
		createTable(columnNames, columnDataTypes, false);
	}

	/**
	 * Creates a table in the database with an auto-incrementing ID column; the first specified column name will be the primary key column
	 * If the table already exists, it will be dropped first if force is true.
	 *
	 * @param columnNames the columns to create
	 * @param columnDataTypes the data types of the columns, defined in the same order as the column names
	 * @param force if true, the table will be dropped first if it already exists (directly replacing tables isn't supported on Oracle databases)
	 *
	 * @return true if the table was created successfully, false if it already existed and force was false
	 */
	public boolean createTable(List<String> columnNames, List<Class<?>> columnDataTypes, boolean force)
	{
		if (columnNames.isEmpty())
		{
			throw new IllegalArgumentException("Column names cannot be empty");
		}
		else if (columnDataTypes.isEmpty())
		{
			throw new IllegalArgumentException("Column data types cannot be empty");
		}
		else if (columnNames.size() != columnDataTypes.size())
		{
			throw new IllegalArgumentException("Column names and data types must be the same length");
		}
		else
		{
			boolean exists = exists();
			if (exists && !force) return false; //throw new IllegalStateException("Table already exists and force create was false");
			else if (exists) dropTable();
		}
		ArrayList<String> definedDataTypes = new ArrayList<>();
		columnDataTypes.forEach(columnDataType ->
		{
			if (columnDataType.equals(byte.class) || columnDataType.equals(Byte.class) || columnDataType.equals(short.class) || columnDataType.equals(Short.class) || columnDataType.equals(int.class) || columnDataType.equals(Integer.class) || columnDataType.equals(BigInteger.class) || columnDataType.equals(BigDecimal.class))
			{
				definedDataTypes.add("NUMBER");
			}
			else if (columnDataType.equals(java.sql.Blob.class) || columnDataType.equals(byte[].class))
			{
				definedDataTypes.add("BLOB");
			}
			else if (columnDataType.equals(java.sql.Clob.class))
			{
				definedDataTypes.add("CLOB");
			}
			else if (columnDataType.equals(boolean.class) || columnDataType.equals(Boolean.class))
			{
				definedDataTypes.add("BOOLEAN");
			}
			else if (columnDataType.equals(char.class) || columnDataType.equals(Character.class))
			{
				definedDataTypes.add("CHAR(1)");
			}
			else if (columnDataType.equals(Date.class))
			{
				definedDataTypes.add("DATE");
			}
			else if (columnDataType.equals(Time.class) || columnDataType.equals(Timestamp.class) || columnDataType.equals(java.util.Date.class))
			{
				definedDataTypes.add("TIMESTAMP");
			}
			else if (columnDataType.equals(long.class) || columnDataType.equals(Long.class))
			{
				definedDataTypes.add("LONG");
			}
			else if (columnDataType.equals(float.class) || columnDataType.equals(Float.class) || columnDataType.equals(double.class) || columnDataType.equals(Double.class))
			{
				definedDataTypes.add("FLOAT");
			}
			else if (columnDataType.equals(String.class) || columnDataType.equals(JsonElement.class))
			{
				//definedDataTypes.add("NVARCHAR(2147483647)");
				definedDataTypes.add("VARCHAR2(32767)");
			}
			else
			{
				throw new IllegalArgumentException("Unsupported data type: " + columnDataType.getName());
			}
		});
		StringBuilder sql = new StringBuilder("CREATE TABLE " + table + "\n(\n");
		sql.append("    ID INT GENERATED ALWAYS AS IDENTITY(START with 1 INCREMENT by 1)");
		if (!columnNames.isEmpty()) sql.append(",\n");
		for (int i = 0; i < columnNames.size(); i++)
		{
			sql.append("    ");
			sql.append(columnNames.get(i));
			sql.append(" ");
			sql.append(definedDataTypes.get(i));
			if (i == 0) sql.append(" PRIMARY KEY NOT NULL");
			if (i < columnNames.size() - 1) sql.append(",\n");
			else sql.append("\n");
		}
		sql.append(")");
		CreeperOracleDBPlugin.getInstance().getLogger().info("Creating table: " + sql);
		try
		{
			connection.createStatement().executeUpdate(sql.toString());
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		//connection.createStatement().executeUpdate(sql.toString());
		return true;
	}

	/**
	 * Truncates the table in the database.
	 */
	public void truncateTable()
	{
		try
		{
			connection.createStatement().executeUpdate("TRUNCATE TABLE " + table);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Drops the table from the database.
	 */
	public void dropTable()
	{
		try
		{
			connection.createStatement().executeUpdate("DROP TABLE " + table);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets the number of rows in the table.
	 *
	 * @param column the column name to access
	 * @return the number of rows in the table
	 */
	public int getRowCount(String column)
	{
		try
		{
			ResultSet resultSet = connection.createStatement().executeQuery("SELECT COUNT(" + column + ") FROM " + table);
			if (!resultSet.next()) throw new IllegalStateException("Failed to check row count of table");
			else return resultSet.getInt(1);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Checks if a column exists in the table.
	 *
	 * @param column the column name to check
	 * @return true if the column exists, false otherwise
	 */
	public boolean columnExists(String column)
	{
		try
		{
			ResultSet resultSet = connection.getMetaData().getColumns(null, null, table, column);
			return resultSet.next();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a boolean from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the boolean value from the database, or false if not found
	 */
	public boolean getBoolean(String column, int row)
	{
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getBoolean(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a string from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the string value from the database, or null if not found
	 */
	@Nullable
	public String getString(String column, int row)
	{
		if (row < 0) throw new IllegalArgumentException("Row index must be more than 0");
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getString(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a byte from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the string value from the database, or 0 if not found
	 */
	public byte getByte(String column, int row)
	{
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getByte(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a short from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the short value from the database, or 0 if not found
	 */
	public short getShort(String column, int row)
	{
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getShort(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets an int from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the int value from the database, or 0 if not found
	 */
	public int getInt(String column, int row)
	{
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getInt(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a long from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the long value from the database, or 0 if not found
	 */
	public long getLong(String column, int row)
	{
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getLong(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a float from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the float value from the database, or 0 if not found
	 */
	public float getFloat(String column, int row)
	{
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getFloat(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a double from the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @return the double value from the database, or 0 if not found
	 */
	public double getDouble(String column, int row)
	{
		try (ResultSet resultSet = query(column, row))
		{
			return resultSet.getDouble(column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Saves a boolean to the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @param value the boolean value to save
	 */
	public void saveBoolean(String column, int row, boolean value)
	{
		if (row < 0) throw new IllegalArgumentException("Row index must be more than 0");
		try
		{
			save(column, row, value);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Saves a string to the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @param value the string value to save
	 */
	public void saveString(String column, int row, String value)
	{
		if (row < 0) throw new IllegalArgumentException("Row index must be more than 0");
		try
		{
			save(column, row, value);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Saves a {@link java.lang.Number Number} to the database.
	 *
	 * @param column the column name to access
	 * @param row the row index, starting from 0
	 * @param value the byte value to save
	 */
	public void saveNumber(String column, int row, Number value)
	{
		if (row < 0) throw new IllegalArgumentException("Row index must be more than 0");
		try
		{
			save(column, row, value);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}


	/**
	 * Removes a column from the table.
	 * 
	 * @param column the column name to remove
	 */
	public void removeColumn(String column)
	{
		try
		{
			connection.createStatement().executeUpdate("ALTER TABLE " + table + " DROP COLUMN " + column);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}


	/**
	 * Removes an row from the table;
	 *
	 * @param row the row index to remove
	 */
	public void removeRow(int row)
	{
		try
		{
			PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM " + table + " WHERE rownum = ?");
			preparedStatement.setInt(1, row);
			preparedStatement.executeUpdate();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets a column of booleans from the database.
	 *
	 * @param column the column name to access
	 * @return a list of booleans from the database, or an empty list if not found
	 */
	public ArrayList<Boolean> getBooleanColumn(String column)
	{
		ArrayList<Boolean> booleans = new ArrayList<>();
		try (ResultSet resultSet = query(column, 0))
		{
			while (resultSet.next())
			{
				booleans.add(resultSet.getBoolean(column));
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return booleans;
	}

	/**
	 * Gets a column of strings from the database.
	 *
	 * @param column the column name to access
	 * @return a list of strings from the database, or an empty list if not found
	 */
	public ArrayList<String> getStringColumn(String column)
	{
		ArrayList<String> strings = new ArrayList<>();
		try (ResultSet resultSet = query(column, 0))
		{
			while (resultSet.next())
			{
				strings.add(resultSet.getString(column));
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return strings;
	}

	/**
	 * Gets a column of {@link java.lang.Number Number}s from the database.
	 *
	 * @param column the column name to access
	 * @return a list of numbers from the database, or an empty list if not found
	 */
	public ArrayList<Number> getNumberColumn(String column)
	{
		ArrayList<Number> numbers = new ArrayList<>();
		try (ResultSet resultSet = query(column, 0))
		{
			while (resultSet.next())
			{
				numbers.add(resultSet.getBigDecimal(column));
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return numbers;
	}

	@NotNull
	private ResultSet query(String column, int row) throws SQLException
	{
		ResultSet resultSet = connection.createStatement().executeQuery("SELECT " + column + " FROM " + table);
		for (int index = 0; index < row; index++)
		{
			if (!resultSet.next()) throw new IllegalStateException("Row index out of bounds");
		}
		return resultSet;
	}


	private void save(String column, int row, Object value) throws SQLException
	{
		if (row < 0) throw new IllegalArgumentException("Row index must be more than 0");
		if (row < getRowCount(column))
		{
			connection.createStatement().executeUpdate("UPDATE " + table + " SET " + column + "=" + (value instanceof String string ? "'" + string + "'" : value) + " WHERE rownum = " + row);
		}
		else connection.createStatement().executeUpdate("INSERT INTO " + table + " (" + column + ") VALUES (" + (value instanceof String string ? "'" + string + "'" : value) + ")");
	}
    /*
	@Nullable
	public OracleCollection getCollection(String name) throws OracleException
	{
		return database.openCollection(name);
	}


	@NotNull
	public OracleCollection createCollection(String name) throws OracleException
	{
		return database.admin().createCollection(name, client.createMetadataBuilder().keyColumnAssignmentMethod("client").build());
	}

	public OracleDocument createDocument(String name, String json, String contentType) throws OracleException
	{
		return database.createDocumentFromString(name, json, contentType);
	}
	*/


	/**
	 * Closes the active connection;
	 *
	 * @throws SQLException if a database error occurs
	 */
	public void close() throws SQLException
	{
		if (connection != null) connection.close();
	}

	/**
	 * Closes all registered connections.
	 */
	public static void closeAllConnections()
	{
		for (Connection connection : CONNECTIONS)
		{
			try
			{
				connection.close();
			}
			catch (SQLException e)
			{
				Utility.printException(CreeperOracleDBPlugin.getInstance().getLogger(), e);
				e.printStackTrace();
			}
		}
		CONNECTIONS.clear();
	}
}
