#!/bin/bash


# For root backup
source_pool='xeonroot'
dest_pool='xeonpool'
backup_subdir='BACKUP'

# For srv-backup1
#source_pool='datapool'
#dest_pool='srv-backup1'

# For srv-backup2
#source_pool='datapool'
#dest_pool='srv-backup2'

######## Functions ########
YELLOWCOLOR='\e[0;33m'
GREENCOLOR='\e[0;32m'
REDCOLOR='\e[0;31m'
ENDCOLOR='\e[0m'

yellow() (
        echo -e "${YELLOWCOLOR}${@}${ENDCOLOR}"
)
green() (
        echo -e "${GREENCOLOR}${@}${ENDCOLOR}"
)
red() (
        echo -e "${REDCOLOR}${@}${ENDCOLOR}"
)

empty_check() {
        if [[ -z "$1" ]]; then
                echo "ERROR: empty string"
                exit 1
        fi
}

######## Argument Parsing ########
force_rollback=N
warnings_printed=N
errors_printed=N

while getopts ":i:o:f" opt; do
        case $opt in
                i)
                        source_pool="$OPTARG"
                ;;
                o)
                        dest_pool="$OPTARG"
                ;;
                f)
                        force_rollback="Y"
                ;;
                \?)
                        echo "Invalid option: -$OPTARG" >&2
                        exit 1
                ;;
                :)
                        echo "Option -$OPTARG requires an argument." >&2
                        exit 1
                ;;
        esac
done

echo "Parameters:"
echo -n "  source_pool:    "
green "$source_pool"
echo -n "  dest_pool:      "
green "$dest_pool"
echo -n "  force_rollback: "
if [[ "$force_rollback" == "Y" ]]; then
        red "Yes"
else
        green "No"
fi
echo

echo "Continue? (y/n)"
echo -n ">"
read resp
if [[ "$resp " != "y " ]]; then
        red "Aborted"
        exit 1
fi

#### Script starts here

if [[ -z "$source_pool" ]]; then
        red "$source_pool must not be empty"
        exit 1
fi
if [[ -z "$dest_pool" ]]; then
        red "$dest_pool must not be empty"
        exit 1
fi
if [[ -z "$(zpool list | grep "^${source_pool}[ \t]")" ]]; then
        red "$source_pool not listed with zpool list - is it imported?"
        exit 1
fi
if [[ -z "$(zpool list | grep "^${dest_pool}[ \t]")" ]]; then
        red "$dest_pool not listed with zpool list - is it imported?"
        exit 1
fi

for vol_name in $(zfs list -H -t filesystem,volume -s name -o name | grep "^$source_pool"); do
        echo

        vol_suffix="$(echo $vol_name | sed -e "s|^$source_pool/||g")"
        empty_check "$vol_name"
        empty_check "$vol_suffix"

        vol_type="$(zfs get type "${vol_name}" -o value -H)"
        vol_canmount="$(zfs get canmount "${vol_name}" -o value -H)"
        vol_mountpoint="$(zfs get mountpoint "${vol_name}" -o value -H)"
        vol_autosnapshots="$(zfs get com.sun:auto-snapshot "${vol_name}" -o value -H)"
        vol_num_snapshots="$(zfs list -H -t snapshot -r "${vol_name}" | grep "^${vol_name}@" | wc -l)"
        vol_dest="${dest_pool}/${backup_subdir}/${vol_name}"

        if [[ "$vol_suffix" =~ ^docker ]]; then
                yellow "Skipping docker volume $vol_name"
                continue
        fi

        echo "Processing vol_name=$vol_name"
        echo "  vol_suffix=$vol_suffix"
        echo "  vol_type=$vol_type"
        echo "  vol_canmount=$vol_canmount"
        echo "  vol_mountpoint=$vol_mountpoint"
        echo "  vol_autosnapshots=$vol_autosnapshots"
        echo "  vol_num_snapshots=$vol_num_snapshots"
        echo "  vol_dest=$vol_dest"

        empty_check "$vol_dest"

        # Make sure there are no volumes with auto snapshots enabled but canmount=off
        if [[ "$vol_type" == "filesystem" ]]; then
                if [[ "$vol_canmount" == "off" ]] && [[ "$vol_autosnapshots" == "true" ]]; then
                        red "????????? canmount=off AND vol_autosnapshots=true ?????????"
                        exit 1
                fi
# TODO: Rethink this check? Lots of backups of backups are mountable but not mounted
#               if [[ "$vol_canmount" == "on" ]] && [[ "$vol_mountpoint" == "none" ]]; then
#                       yellow "Skipping volume $vol_name as it has canmount=on and mountpoint=none"
#                       continue
#               fi
        fi

        # Check if this volume exists on the destination pool
        if [[ -z "$(zfs list -H -t filesystem,volume -o name | grep "^${vol_dest}\$")" ]]; then
                if [[ "$vol_type" == "filesystem" ]]; then
                        if [[ "$vol_num_snapshots" -gt 0 ]]; then
                                echo
                                red "  WARNING - Dataset not found on destination pool - it must be created"
                                red "  To do this, use the following command"
                                red "  Consider adding '-o readonly=on -o compression=lz4 -o atime=off -o com.sun:auto-snapshot=false' if this is the top-level dataset (it inherits)"
                                if [[ "$vol_canmount" == "off" ]]; then
                                        red "  zfs create -o canmount=off -o mountpoint=none ${vol_dest}"
                                else
                                        #red "  zfs create -o mountpoint=/${source_pool}${vol_mountpoint} ${vol_dest}"
                                        # Is there any reason we need to specify the mountpoint for backups? I think no...
                                        red "  zfs create ${vol_dest}"
                                fi
                        else
                                green "  Skipping dataset that doesn't exist on the destination as it has no snapshots"
                        fi
                else
                        volsize="$(zfs get volsize "${vol_name}" -o value -H)"
                        volblocksize="$(zfs get volblocksize "${vol_name}" -o value -H)"
                        red "  WARNING - Dataset not found on destination pool - it must be created"
                        red "  To do this, use the following command"
                        red "  zfs create -V ${volsize} -b ${volblocksize} -o refreservation=none -o reservation=none ${vol_dest}"
                fi
                errors_printed=Y
                continue
        fi

        # No more processing needed for volumes that don't have any data
        if [[ "$vol_num_snapshots" -eq 0 ]]; then
                green "  Skipping send due to no snapshots present"
                continue
        fi
        # Volumes exist - need to send snapshots

        ###################################################################################
        ###### Find the newest snapshot that is in both source and destination pools ######

        dest_snaps="$(zfs list -H -t snapshot -o name -r "${vol_dest}" | grep "^${vol_dest}@" | tac)"
        source_snaps="$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${vol_name}@")"
        common_snap=""

        # echo "  Destination snaps: $dest_snaps"
        # echo "  Source snaps: $source_snaps"

        # Snapshots are in reverse order, from newest to oldest for performance.
        for testsnap in $dest_snaps; do
                snapname="$(echo "$testsnap" | sed -e "s|^${vol_dest}@||g")"

                # Top level check is a fast search on strings
                if [[ ! -z "$( echo "$source_snaps" | grep "$snapname")" ]]; then
                        # Do a full query to make really sure it exists in the source
                        if [[ ! -z "$(zfs list -H -t snapshot -o name -r "${vol_dest}" | grep "^${vol_dest}@${snapname}$")" ]] && \
                           [[ ! -z "$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${vol_name}@${snapname}$")" ]]; then
                                # echo "Found common snap $snapname"
                                common_snap="$snapname"
                                break;
                        fi
                fi
        done

        ###################################################################################
        ###### If no valid snapshots exist for an incremental send, prompt           ######
        if [[ -z "$common_snap" ]]; then
                # Choose the oldest snapshot available
                initial_snap="$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${vol_name}@" | head -n 1)"

                # Must find at least something
                empty_check "$initial_snap"

                # Tested that this preserves permissions if created before
                echo
                red "  WARNING - No common snapshots found between source and destination datasets"
                red "  The only way to proceed is to forcibly overwrite the destination"
                red "  Serious danger here! If sure, run this:"
                red "    zfs send '$initial_snap' | zfs recv -v '${vol_dest}' -F"
                errors_printed=Y
                continue
        fi

        # Can only get here if common_snap is not empty
        empty_check "$common_snap"
        echo "  common_snap=${common_snap}"

        ###################################################################################
        ###### If common snapshot isn't most recent in destination, prompt           ######
        snaplist=$(zfs list -H -t snapshot -o name -r "${vol_dest}" | grep "^$vol_dest@")
        latest_absolute="$(echo "$snaplist" | tail -n 1)"
        empty_check "$latest_absolute"

        if [[ "$latest_absolute" != "${vol_dest}@${common_snap}" ]]; then
                # About to ask ZFS to rollback - need to *really* sanity check
                empty_check "${source_pool}"
                empty_check "${dest_pool}"
                empty_check "${dest_pool}/${backup_subdir}/${source_pool}"
                empty_check "$(echo "${vol_dest}" | grep "^${dest_pool}/${backup_subdir}/${source_pool}")"

                echo
                yellow "  Latest destination snapshot not the same as latest common snapshot"
                yellow "  Need to roll back until the latest snapshot was the most recent common snapshot"
                yellow "  Lastest absolute snapshot: $latest_absolute"
                rollback_command="zfs rollback ${vol_dest}@${common_snap}"
                yellow "  Rollback command: $rollback_command"

                rollback_output=$($rollback_command 2>&1)
                if [[ -z $(echo "$rollback_output" | head -n 1 | grep "^cannot rollback to '${vol_dest}@${common_snap}': more recent snapshots or bookmarks exist\$") ]]; then
                        red "  DANGER: First line mismatch on zfs rollback output! Got:"
                        red "    $(echo "$rollback_output" | head -n 1)"
                        exit 1
                fi
                if [[ -z $(echo "$rollback_output" | head -n 2 | tail -n 1 | grep "^use '-r' to force deletion of the following snapshots and bookmarks:\$") ]]; then
                        red "  DANGER: Second line mismatch on zfs rollback output! Got:"
                        red "    $(echo "$rollback_output" | head -n 2 | tail -n 1)"
                        exit 1
                fi

                expected_rollbacklist=$(echo "$snaplist" | sed "0,\:^${vol_dest}@${common_snap}$:d")
                empty_check "${expected_rollbacklist}"
                # TODO: zfs rollback now only reports up to 32 snaps that would be destroyed, so this diff mechanism no longer works
                #if [[ "$(echo "${expected_rollbacklist}" | sort)" != "$(echo "$rollback_output" | tail -n '+3' | sort)" ]]; then
                #       red "  DANGER: zfs rollback reports different list of snapshots than manual calculation"
                #       red "  Expected:"
                #       red "$(echo "${expected_rollbacklist}" | sort)"
                #       echo
                #       red "  Got:"
                #       red "$(echo "$rollback_output" | tail -n '+3' | sort)"
                #       exit 1
                #fi

                if [[ ! -z "$(echo "$expected_rollbacklist" | grep -i "monthly")" ]]; then
                        red "  DANGER: zfs rollback would destroy monthly snapshot!"
                        red "  List of snapshots that would be destroyed:"
                        red "$expected_rollbacklist"
                        red "  If this is ok, manually execute:"
                        red "    zfs rollback ${vol_dest}@${common_snap}"
                        errors_printed=Y
                        continue
                fi

                if [[ "$force_rollback" == "Y" ]]; then
                        yellow "  Executing: zfs rollback ${vol_dest}@${common_snap} -r"
                        empty_check "${vol_dest}"
                        empty_check "${common_snap}"
                        zfs rollback "${vol_dest}@${common_snap}" -r
                        if [[ "$?" -ne 0 ]]; then
                                red "Something went wrong! Aborting..."
                                exit 1
                        fi
                else
                        yellow "  Execute this line or restart this script with -f"
                        yellow "    zfs rollback ${vol_dest}@${common_snap} -r"
                        warnings_printed=Y
                        continue
                fi
        fi

        ###################################################################################
        ###### Send all intermediate snapshots starting from the common one          ######

        snaps_to_add="$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${vol_name}@" | sed "0,\:^${vol_name}@${common_snap}$:d" )"

        # Do some junk to figure out what the last non-monthly auto snapshot was,
        last_non_monthly_auto="$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${vol_name}@" | sed "0,\:^${vol_name}@${common_snap}$:d" | grep -E "zfs-auto-snap_(weekly|daily|hourly|frequent)" | tail -n 1 )"
        if [[ ! -z "$last_non_monthly_auto" ]]; then
                # Take everything before that last non-monthly auto snapshot, filter out all the non-monthly snapshots, then combine that with the last non-monthly auto snapshot and everything that comes after
                before_last_non_monthly_auto="$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${vol_name}@" | sed "0,\:^${vol_name}@${common_snap}$:d" | sed -n "\:^${last_non_monthly_auto}$:q;p" | grep -vE 'zfs-auto-snap_(weekly|daily|hourly|frequent)')"
                after_last_non_monthly_auto="$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${vol_name}@" | sed "0,\:^${vol_name}@${common_snap}$:d" | sed "0,\:^${last_non_monthly_auto}$:d" )"
                snaps_to_add="$before_last_non_monthly_auto $last_non_monthly_auto $after_last_non_monthly_auto"
        fi

        if [[ ! -z "${snaps_to_add}" ]]; then
                for inc_add_snap in ${snaps_to_add}; do
                        empty_check "${inc_add_snap}"
                        empty_check "${common_snap}"
                        empty_check "${vol_dest}"

                        echo -n "  Sending incremental snapshot: "
                        green "${inc_add_snap}"

                        # Make sure the snapshot still exists by the time we get to it, otherwise skip it
                        if [[ -z "$(zfs list -H -t snapshot -o name -r "${vol_name}" | grep "^${inc_add_snap}\$")" ]]; then
                                continue
                        fi

                        zfs send -i "${common_snap}" "${inc_add_snap}" | zfs recv "${vol_dest}"
                        if [[ "$?" -ne 0 ]]; then
                                red "Something went wrong! Aborting..."
                                exit 1
                        fi
                        common_snap="${inc_add_snap}"
                done
        else
                green "  Already up to date"
        fi
done

echo
echo
if [[ "$warnings_printed" != "N" ]]; then
        yellow "Warnings were generated!"
fi
if [[ "$errors_printed" != "N" ]]; then
        red "Critical warnings were generated!"
fi
echo "Backup complete"
