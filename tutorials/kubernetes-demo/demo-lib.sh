#!/usr/bin/env bash

# Script inspired by https://github.com/paxtonhare/demo-magic
# (https://github.com/paxtonhare/demo-magic/issues/15)
#
# Example usage in script:
#  #!/usr/bin/env bash
#
#  . demo-lib.sh
#
# clear
#
# r "ls -l"
# r "(...)"
# rc "eval and clean"
# r "echo \"The end!\" "
#
# navigate

# the speed to "type" the text
TYPE_SPEED=40

# handy color vars for pretty prompts
BLACK="\033[0;30m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
GREEN="\033[0;32m"
CYAN="\033[0;36m"
RED="\033[0;31m"
PURPLE="\033[0;35m"
BROWN="\033[0;33m"
WHITE="\033[1;37m"
COLOR_RESET="\033[0m"

function check_pv() {
  command -v pv >/dev/null 2>&1 || {

    echo ""
    echo -e "${RED}##############################################################"
    echo "# HOLD IT!! I require pv but it's not installed.  Aborting." >&2;
    echo -e "${RED}##############################################################"
    echo ""
    echo -e "${COLOR_RESET}Installing pv:"
    echo ""
    echo -e "${BLUE}Mac:${COLOR_RESET} $ brew install pv"
    echo ""
    echo -e "${BLUE}Other:${COLOR_RESET} http://www.ivarch.com/programs/pv.shtml"
    echo -e "${COLOR_RESET}"
    exit 1;
  }
}

CMDS=()
CLEAN_AFTER=()

##
# Registers a command for navigate mode.
#
# takes 1 parameter - the string command to run
#
# usage: r "ls -l"
#
##
function r() {
  CMDS+=("$@")
  CLEAN_AFTER+=(false)
}

##
# Registers a command for navigate mode.
# It registers it with clean_after attribute which will not show it after evaluation.
#
# takes 1 parameter - the string command to run
#
# usage: rc "ls -l"
#
##
function rc() {
  CMDS+=("$@")
  CLEAN_AFTER+=(true)
}

##
# Runs in a mode that enables easy navigation of the commands in the sequential manner.
#
# TODO: Add search (ctlr+r) functionality
##
function navigate() {
  curr=0

  while true
  do
    # Check boundaries.
    if (( ${curr} < 0 )); then
      curr=0
    fi
    if (( ${curr} >= ${#CMDS[@]} )); then
      curr=${#CMDS[@]}-1
    fi

    cmd=${CMDS[${curr}]}

    # Make sure input will not break the print.
    stty -echo
    if [[ -z $TYPE_SPEED ]]; then
      echo -en "${YELLOW}$cmd${COLOR_RESET}"
    else
      echo -en "${YELLOW}$cmd${COLOR_RESET}" | pv -qL $[$TYPE_SPEED+(-2 + RANDOM%5)];
    fi
    stty echo

    # Ignore accidently buffered input (introduces 0.5 input lag).
    read -rst 0.3 -n 10000 discard

    # Is this the command we want to run?
    read -rs -n1 input
    case $(printf "%X" \'${input}) in
    '62') # b - skip this command and move to beginning.
      curr=0
      echo -en "\033[2K\r"
      ;;
    '65') # e - skip this command and move to the end.
      curr=${#CMDS[@]}-1
      echo -en "\033[2K\r"
      ;;
    '6E') # n - skip this command and move to next.
      ((curr++))
      echo -en "\033[2K\r"
      ;;
    '70') # p - skip this command and move to previous.
      ((curr--))
      echo -en "\033[2K\r"
      ;;
    '0') # enter - eval this and move to next.
      if ${CLEAN_AFTER[${curr}]}; then
        echo -en "\033[2K\r"
      else
        echo ""
      fi
      ((curr++))
      out=$(eval ${cmd})
      if [[ ${CLEAN_AFTER[${curr}]} == false ]]; then
        echo ${out}
      fi
      ;;
    '71'|'1B') # q or escape - exit.
      echo ""
      echo "Bye!"
      exit 0
      ;;
    *)  # print again - not supported input.
      echo -en "\r"
      ;;
    esac
  done
}

check_pv