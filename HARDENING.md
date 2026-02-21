# Hardening Guide: Secure Remote Backups

By default the README example uses `user: root` on the destination. This works,
but gives the source machine full root access to the destination over SSH — a
poor security posture for an automated, unattended process.

This guide shows how to reach a much stronger posture:

- No root SSH access required
- SSH key scoped to a single source IP
- ZFS permissions limited to the backup dataset subtree only
- No `sudo`, no `NOPASSWD` entries

---

## Overview

`mzb` runs entirely on the **source** machine. For remote destinations it SSHes
in and runs a small set of ZFS commands:

| Command | Purpose |
|---|---|
| `zfs list` | Enumerate existing snapshots |
| `zfs recv` | Receive an incremental send stream |
| `zfs rollback -r` | Roll back to latest common snapshot |
| `zfs destroy` | Prune old snapshots during compaction |

ZFS's built-in delegated administration (`zfs allow`) lets you grant a
non-root user exactly these permissions on a specific dataset subtree, enforced
at the kernel level — no elevated shell access required.

---

## Step 1: Create a dedicated user on the destination

On the destination server and/or the source machine:
```
useradd -m -s /usr/bin/rbash mzb
```

`rbash` is a somewhat more restricted shell that limits the user's ability to
change directories or execute arbitrary commands. This provides possibly some
additional security vs `bash`. `-m` creates a home directory for the user.

---

## Step 2: Delegate ZFS permissions

Grant the `mzb` user exactly the permissions it needs, scoped to your backup
dataset subtree.

On the sender, this should be at least:
```
sudo zfs allow -u mzb send,hold sourcepool
```

On the receiver, this should be at least:
```
sudo zfs allow -u mzb mount,hold,create,receive,rollback,destroy destpool/BACKUP
```

Replace `destpool/BACKUP` with your actual `pool/prefix` combination (matching
`pool` + `prefix` in your job YAML). Permissions are inherited by all child
datasets, so this single command covers every dataset `mzb` will ever create
under that prefix.

Note that `destroy` requires the `mount` permission, `send` and `receive`
require the `hold` permission, and `receive` requires the `create` permission.

Verify at any time:

```
zfs allow destpool/BACKUP
```

---

## Step 3: Generate a dedicated SSH key on the source

Use a separate key for each backup job — never reuse your personal key for
automated processes:

```
ssh-keygen -t ed25519 -f ~/.ssh/id_mzb_desktop_to_server -C "mzb@desktop-backup" -N ""
```

The `-N ""` sets an empty passphrase so cron can use it without an agent. For
added security, you can use a password and make the key available via an SSH
agent, but you will need to automate unlocking the key file on reboot prior to
running `mzb`.

---

## Step 4: Install the key with restrictions on the destination

Add the public key to `/home/mzb/.ssh/authorized_keys` (create the file if
needed, with permissions `600`, owned by `mzb`):

```
restrict,from="192.168.1.10" ssh-ed25519 AAAA... mzb@desktop-backup
```

Two important restrictions:

- **`restrict`** — modern OpenSSH shorthand that disables PTY allocation, agent
  forwarding, port forwarding, and X11 forwarding. The backup process needs
  none of these.
- **`from="..."`** — locks the key to connections originating from your source
  machine's IP. A stolen or leaked key is useless from any other host.

Use the actual IP address rather than a hostname to avoid DNS spoofing. If your
source has multiple interfaces, list all relevant IPs:
`from="192.168.1.10,10.0.0.5"`.

---

## Step 5: Optionally harden `sshd_config` on the destination

Add a `Match User` block to enforce restrictions at the server level,
regardless of what the client requests:

```
Match User mzb
    AllowAgentForwarding no
    AllowTcpForwarding no
    X11Forwarding no
    PermitTTY no
```

Reload sshd after editing: `systemctl reload sshd`.

This is defense-in-depth on top of `authorized_keys`

---

## Step 6: Update your job YAML

Change `user: root` to `user: mzb` (or whichever username you chose):

```yaml
destination:
  pool: destpool
  prefix: BACKUP
  host: server.local
  user: mzb          # ← was: root
  port: 22
```

---

## Step 7: Configure the SSH key for cron

The cron job needs to present the right key. The simplest approach is to add a
`~/.ssh/config` entry for the `mzb` user:
```
Host server.local
    IdentityFile ~/.ssh/id_mzb_desktop_to_server
    IdentitiesOnly yes
```

Using a key with a passphrase will require running the agent and loading the key
into it, and then ensuring the cron process has access to the agent socket and
can find it via the `SSH_AUTH_SOCK` environment variable.

---

## Step 8: Add a cron entry

Run `crontab -e` as the user on the source machine who owns the SSH key and the
`mzb` virtualenv. Because cron runs with a minimal environment, use the full
path to the `mzb` binary:

```
0 3 * * * /home/user/.venv/bin/mzb backup /etc/mzb/desktop-to-server.yaml --no-confirm
```

A few things to keep in mind:

- **`--no-confirm`** is required — cron is non-interactive, so any prompt would
  cause the job to hang or abort.
- **Full path to `mzb`** — cron's `PATH` won't include your virtualenv. Use the
  absolute path to the binary inside your venv, or set `PATH` at the top of the
  crontab.
- **No passphrase / no agent needed** — if you followed Step 3 and generated the
  key with `-N ""`, and Step 7's `~/.ssh/config` is in place, SSH will pick up
  the right key automatically with no further configuration.
- **Output** — by default cron emails stdout/stderr to the local user. To log to
  a file instead: append `>> /var/log/mzb.log 2>&1`. To send to the system
  journal: pipe through `systemd-cat -t mzb`.
- **`compact` as a separate job** — if you use compaction, add a second entry on
  a less frequent schedule, e.g. weekly:

```
0 4 * * 0 /home/user/.venv/bin/mzb compact /etc/mzb/desktop-to-server.yaml --no-confirm
```

---

## Verification

Test before committing to cron:

```
# Should succeed — lists snapshots on destination
ssh mzb@server.local zfs list -H -t snapshot destpool/BACKUP

# Dry-run the full backup job
mzb backup desktop-to-server.yaml --dry-run --verbose
```

Check that the `mzb` user cannot do anything outside its sandbox:

```
# Should fail with permission denied
ssh mzb@server.local zfs list rpool
ssh mzb@server.local zfs destroy destpool/someother@snap
```

---

## Result

With this setup, the automated backup process has:

- **No root access** on the destination
- **No sudo** required anywhere
- **Scoped ZFS permissions** — only `destpool/BACKUP` and its children
- **IP-locked SSH key** — useless if stolen
- **No interactive shell** — the `mzb` user cannot be used for anything else

The attack surface for a compromised source machine is limited to the backup
dataset subtree on the destination. Primary data on the destination is
untouched.
```

Now I'll update the README to link to it:
