package creeperpookie.creeperoracledbplugin.handlers;

import creeperpookie.creeperoracledbplugin.CreeperOracleDBPlugin;
import org.bukkit.Bukkit;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PlayerHandler implements Listener
{
	@EventHandler
	public void onPlayerJoin(PlayerJoinEvent event)
	{
		savePlayers();
	}

	@EventHandler
	public void onPlayerQuit(PlayerJoinEvent event)
	{
		savePlayers();
	}

	private static void savePlayers()
	{
		if (!CreeperOracleDBPlugin.isTableManagerReady()) return;
		else if (CreeperOracleDBPlugin.getInstance().getConfig().getBoolean("create-table", false)) CreeperOracleDBPlugin.getTableManager().createTable(List.of("players"), List.of(String.class));
		CreeperOracleDBPlugin.getTableManager().truncateTable();
		AtomicInteger index = new AtomicInteger(0);
		CreeperOracleDBPlugin.getInstance().getLogger().info("Saving players to database: ");
		CreeperOracleDBPlugin.getTableManager().truncateTable();
		Bukkit.getOnlinePlayers().forEach(player -> CreeperOracleDBPlugin.getTableManager().saveString("players", index.getAndIncrement(), player.getName()));
		CreeperOracleDBPlugin.getTableManager().getStringColumn("players").forEach((player) -> CreeperOracleDBPlugin.getInstance().getLogger().info(player));
	}
}
