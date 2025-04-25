package creeperpookie.creeperoracledbplugin;

import creeperpookie.creeperoracledbplugin.api.TableManager;
import creeperpookie.creeperoracledbplugin.command.SodaCommand;
import creeperpookie.creeperoracledbplugin.handlers.PlayerHandler;
import creeperpookie.creeperoracledbplugin.util.Utility;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.Random;

public class CreeperOracleDBPlugin extends JavaPlugin
{
	private static CreeperOracleDBPlugin instance;
	private static TableManager tableManager;
	private static final Random RANDOM = new Random();

	@Override
	public void onEnable()
	{
		instance = this;
		loadConfig();
		Bukkit.getPluginManager().registerEvents(new PlayerHandler(), this);
		getCommand("creeperoracledb").setExecutor(new SodaCommand());
	}

	@Override
	public void onDisable()
	{
		// Plugin shutdown logic
		getCommand("creeperoracledb").unregister(Bukkit.getCommandMap());
		TableManager.closeAllConnections();
	}

	public void loadConfig()
	{
		saveDefaultConfig();
		reloadConfig();
		String tnsConnectionString = getConfig().getString("tns-connection-string");
		if (tnsConnectionString == null || tnsConnectionString.isEmpty())
		{
			getLogger().severe("TNS connection string is not set in the config.yml");
			return;
		}
		String username = getConfig().getString("database-username");
		if (username == null || username.isEmpty())
		{
			getLogger().severe("Database username is not set in the config.yml");
			return;
		}
		String password = getConfig().getString("database-password");
		if (password == null || password.isEmpty())
		{
			getLogger().severe("Database password is not set in the config.yml");
			return;
		}
		String tableName = getConfig().getString("table-name");
		if (tableName == null || tableName.isEmpty())
		{
			getLogger().severe("Table name is not set in the config.yml");
			return;
		}
		else if (tableName.matches("[^a-zA-Z0-9_#$]"))
		{
			getLogger().severe("Table name can only contain alphanumeric characters, underscores hashtag, and dollar signs");
			return;
		}
		try
		{
			TableManager.closeAllConnections();
			tableManager = new TableManager(tnsConnectionString, username, password, tableName);
		}
		catch (Exception e)
		{
			getLogger().severe("An error occurred initializing the database:");
			Utility.printException(getLogger(), e);
		}
	}

	public static CreeperOracleDBPlugin getInstance()
	{
		return instance;
	}

	public static boolean isTableManagerReady()
	{
		return tableManager != null;
	}

	public static TableManager getTableManager()
	{
		return tableManager;
	}

	public static Random getRandom()
	{
		return RANDOM;
	}
}
