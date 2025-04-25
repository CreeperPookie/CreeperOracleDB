package creeperpookie.creeperoracledbplugin.database;

import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.PriorityBlockingQueue;

public class SQLCommandQueue extends PriorityBlockingQueue<SQLCommand>
{
	/**
	 * Adds a command to the queue.
	 *
	 * @param connection the connection to the database
	 */
	@Nullable
	public void runNext(Connection connection)
	{
		if (this.isEmpty()) throw new IllegalStateException("Queue is empty");
		try
		{
			if (!connection.isValid(5)) throw new IllegalArgumentException("Database connection is not valid");
			SQLCommand command = poll();
			if (command != null)
			{
				Statement statement = connection.createStatement();
				statement.execute(command.toString());
				if (command.isQuery()) command.resultFuture().complete(statement.getResultSet());
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}
}
