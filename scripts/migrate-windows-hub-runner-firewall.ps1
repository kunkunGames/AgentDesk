#Requires -Version 5.1
#Requires -RunAsAdministrator
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)][string]$RuntimeRoot,
    [Parameter(Mandatory = $true)][System.Net.IPAddress[]]$HubAddress,
    [ValidateRange(1, 65535)][int]$Port = 8791
)

$ErrorActionPreference = 'Stop'
$binary = [IO.Path]::GetFullPath((Join-Path $RuntimeRoot 'bin\agentdesk.exe'))
if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
    throw "AgentDesk executable is missing: $binary. Pass the existing release directory as -RuntimeRoot (for example C:\Users\12336\.adk\release)."
}
$oldName = "AgentDeskWorker-TCP-$Port"
$newName = "AgentDeskAPI-TCP-$Port"
$old = Get-NetFirewallRule -Name $oldName -ErrorAction SilentlyContinue
if ($old) {
    $app = $old | Get-NetFirewallApplicationFilter
    $portFilter = $old | Get-NetFirewallPortFilter
    $addresses = @($old | Get-NetFirewallAddressFilter | Select-Object -ExpandProperty RemoteAddress)
    $expected = @($HubAddress | ForEach-Object { $_.ToString() } | Sort-Object -Unique)
    if ($old.Group -ne 'AgentDesk' -or $app.Program -ne $binary -or
        $portFilter.Protocol -notin @('TCP', '6') -or [string]$portFilter.LocalPort -ne [string]$Port -or
        (Compare-Object ($addresses | Sort-Object -Unique) $expected)) {
        throw 'Existing rule does not match the requested installation and hub scope.'
    }
}
if ($PSCmdlet.ShouldProcess("$oldName -> $newName", 'Replace the exact AgentDesk firewall rule')) {
    & (Join-Path $PSScriptRoot 'install-windows-runner-firewall.ps1') -RuntimeRoot $RuntimeRoot -HubAddress $HubAddress -Port $Port
    $new = Get-NetFirewallRule -Name $newName -ErrorAction Stop
    $app = $new | Get-NetFirewallApplicationFilter
    $addresses = @($new | Get-NetFirewallAddressFilter | Select-Object -ExpandProperty RemoteAddress)
    $expected = @($HubAddress | ForEach-Object { $_.ToString() } | Sort-Object -Unique)
    if ($app.Program -ne $binary -or $new.Enabled -ne 'True' -or
        (Compare-Object ($addresses | Sort-Object -Unique) $expected)) {
        throw 'Replacement rule failed verification; previous rule retained.'
    }
    if ($old) { $old | Remove-NetFirewallRule -ErrorAction Stop }
    Get-NetFirewallRule -Name $newName | Select-Object Name, DisplayName, Enabled, Direction, Action
}
