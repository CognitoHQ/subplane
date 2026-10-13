#pragma once

// Subcommands of the `subplane` CLI. Each lives in its own file with its own
// argument parsing; argv[0] is the subcommand name. The dispatcher is subplane.cpp.

int cmd_probe(int argc, char** argv);
int cmd_tail(int argc, char** argv);
int cmd_schema(int argc, char** argv);
int cmd_replay(int argc, char** argv);
int cmd_send(int argc, char** argv);
int cmd_dbn_sim(int argc, char** argv);
int cmd_soak(int argc, char** argv);
#if SUBPLANE_WITH_AERON
int cmd_aeron_pub(int argc, char** argv);
#endif
