package creeperpookie.creeperoracledbplugin.command;

import creeperpookie.creeperoracledbplugin.CreeperOracleDBPlugin;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class SodaCommand implements CommandExecutor, TabCompleter
{
	@Override
	public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String @NotNull [] args)
	{
		if (args.length < 1)
		{
			printHelp(sender, command, label, args);
			return true;
		}
		else switch (args[0].toLowerCase())
		{
			case "reload" ->
			{
				CreeperOracleDBPlugin.getInstance().loadConfig();
				sender.sendMessage("Reloaded Soda configuration");
				return true;
			}
			default ->
			{
				printHelp(sender, command, label, args);
				return true;
			}
		}
	}

	@Override
	@Nullable
	public List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String @NotNull [] args)
	{
		return switch (args.length)
		{
			case 1 -> List.of("reload");
			default -> List.of();
		};
	}

	private void printHelp(CommandSender sender, Command command, String label, String[] args)
	{
		sender.sendMessage("Usage: /" + label + " reload - Reload the plugin configuration");
	}
}
