package creeperpookie.creeperoracledbplugin.database;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.util.concurrent.CompletableFuture;

/**
 * This class is used to represent an SQL command.
 * It contains the type of the command and the statement itself.
 * The type is an integer that corresponds to the type of Statement to execute:
 * <p>
 * 0: Query
 * 1: Update
 * 2: Batch
 */
public record SQLCommand(int type, String sqlCommand,
                         @Nullable CompletableFuture<ResultSet> resultFuture) implements Comparable<SQLCommand>
{
	public SQLCommand(int type, String sqlCommand)
	{
		this(type, sqlCommand, null);
	}

	public boolean isQuery()
	{
		return type == 0;
	}

	public boolean isUpdate()
	{
		return type == 1;
	}

	public boolean isBatch()
	{
		return type == 2;
	}

	@Override
	public int compareTo(@NotNull SQLCommand sqlCommand)
	{
		return this.sqlCommand.compareTo(sqlCommand.sqlCommand());
	}
}
