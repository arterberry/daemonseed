package main

import "github.com/arterberry/daemonseed/internal/roles"

// Slash command prompts installed into .claude/commands/ (spec §8.2).
// bus-list and bus-whoami wrap common tools, so both roles get them; the
// rest are split by the role's tool surface.
const (
	cmdBusList = `List all connected daemonSeed clients. Use the bus_list_all MCP tool and
present the results in a clean table showing: name, role, client_id (short),
state, and last seen time.
`
	cmdBusSend = `Send a direct message via daemonSeed. Ask the user for the target (name or ID)
and the message content if not already provided. Use the bus_send MCP tool.
Confirm delivery or report the error clearly.
`
	cmdBusBroadcast = `Broadcast a message to all connected child instances via daemonSeed.
Ask for the message content if not provided. Use bus_broadcast MCP tool.
Report how many children received the message.
`
	cmdBusStatus = `Request the current status from a specific child instance. Ask for the
target name if not provided. Use bus_get_status MCP tool. Present the
returned status clearly, noting if the request timed out.
`
	cmdBusAssign = `Assign a task to a child instance via daemonSeed. Ask the user for:
1. Target child name
2. Task instruction (what to do)
3. Any context needed
Then format as a task JSON and call bus_assign_task MCP tool. Report the
assigned task_id for tracking.
`
	cmdBusShutdown = `Initiate a graceful shutdown of all daemonSeed connections. Confirm with
the user before proceeding. Use bus_shutdown MCP tool with a 5-second
timeout. Report which children acknowledged and which timed out.
`
	cmdBusReport = `Report this instance's current status to the parent. Ask for a brief
status message and current state (idle/working/blocked/complete/error)
if not provided. Use bus_report_status MCP tool.
`
	cmdBusWhoami = `Show this instance's daemonSeed identity. Use the bus_whoami MCP tool and
display: name, role, client_id, connection time, and daemon version.
`
)

// slashCommands returns filename → content for the given role.
func slashCommands(role roles.Role) map[string]string {
	common := map[string]string{
		"bus-list.md":   cmdBusList,
		"bus-whoami.md": cmdBusWhoami,
	}
	if role == roles.RoleParent {
		common["bus-send.md"] = cmdBusSend
		common["bus-broadcast.md"] = cmdBusBroadcast
		common["bus-status.md"] = cmdBusStatus
		common["bus-assign.md"] = cmdBusAssign
		common["bus-shutdown.md"] = cmdBusShutdown
	} else {
		common["bus-report.md"] = cmdBusReport
	}
	return common
}
