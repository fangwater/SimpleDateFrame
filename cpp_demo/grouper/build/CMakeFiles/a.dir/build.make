# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/build

# Include any dependencies generated for this target.
include CMakeFiles/a.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/a.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/a.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/a.dir/flags.make

CMakeFiles/a.dir/hello_world.cc.o: CMakeFiles/a.dir/flags.make
CMakeFiles/a.dir/hello_world.cc.o: ../hello_world.cc
CMakeFiles/a.dir/hello_world.cc.o: CMakeFiles/a.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/a.dir/hello_world.cc.o"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/a.dir/hello_world.cc.o -MF CMakeFiles/a.dir/hello_world.cc.o.d -o CMakeFiles/a.dir/hello_world.cc.o -c /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/hello_world.cc

CMakeFiles/a.dir/hello_world.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/a.dir/hello_world.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/hello_world.cc > CMakeFiles/a.dir/hello_world.cc.i

CMakeFiles/a.dir/hello_world.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/a.dir/hello_world.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/hello_world.cc -o CMakeFiles/a.dir/hello_world.cc.s

# Object files for target a
a_OBJECTS = \
"CMakeFiles/a.dir/hello_world.cc.o"

# External object files for target a
a_EXTERNAL_OBJECTS =

a : CMakeFiles/a.dir/hello_world.cc.o
a : CMakeFiles/a.dir/build.make
a : /usr/local/lib/libabsl_strings.a
a : /usr/local/lib/libabsl_time.a
a : /usr/local/lib/libabsl_log_internal_conditions.a
a : /usr/local/lib/libabsl_log_internal_message.a
a : /usr/local/lib/libabsl_examine_stack.a
a : /usr/local/lib/libabsl_log_internal_format.a
a : /usr/local/lib/libabsl_str_format_internal.a
a : /usr/local/lib/libabsl_log_internal_proto.a
a : /usr/local/lib/libabsl_log_internal_log_sink_set.a
a : /usr/local/lib/libabsl_log_internal_globals.a
a : /usr/local/lib/libabsl_log_globals.a
a : /usr/local/lib/libabsl_hash.a
a : /usr/local/lib/libabsl_city.a
a : /usr/local/lib/libabsl_bad_optional_access.a
a : /usr/local/lib/libabsl_bad_variant_access.a
a : /usr/local/lib/libabsl_low_level_hash.a
a : /usr/local/lib/libabsl_synchronization.a
a : /usr/local/lib/libabsl_stacktrace.a
a : /usr/local/lib/libabsl_symbolize.a
a : /usr/local/lib/libabsl_debugging_internal.a
a : /usr/local/lib/libabsl_demangle_internal.a
a : /usr/local/lib/libabsl_graphcycles_internal.a
a : /usr/local/lib/libabsl_malloc_internal.a
a : /usr/local/lib/libabsl_log_sink.a
a : /usr/local/lib/libabsl_log_entry.a
a : /usr/local/lib/libabsl_time.a
a : /usr/local/lib/libabsl_civil_time.a
a : /usr/local/lib/libabsl_time_zone.a
a : /usr/local/lib/libabsl_strerror.a
a : /usr/local/lib/libabsl_strings.a
a : /usr/local/lib/libabsl_strings_internal.a
a : /usr/local/lib/libabsl_base.a
a : /usr/local/lib/libabsl_spinlock_wait.a
a : /usr/local/lib/libabsl_int128.a
a : /usr/local/lib/libabsl_throw_delegate.a
a : /usr/local/lib/libabsl_raw_logging_internal.a
a : /usr/local/lib/libabsl_log_severity.a
a : CMakeFiles/a.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable a"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/a.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/a.dir/build: a
.PHONY : CMakeFiles/a.dir/build

CMakeFiles/a.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/a.dir/cmake_clean.cmake
.PHONY : CMakeFiles/a.dir/clean

CMakeFiles/a.dir/depend:
	cd /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/build /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/build /home/fanghz/Public/data/data_for_perf/cpp_demo/grouper/build/CMakeFiles/a.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/a.dir/depend

