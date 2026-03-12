cmake_minimum_required(VERSION 2.8)
project(castor C)

# Disable C++ compiler check
set(CMAKE_CXX_COMPILER "")
set(CMAKE_CXX_COMPILER_WORKS TRUE)

# Set default compilation options
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GNU_SOURCE -fPIC -Wall -Wunused-but-set-variable -D __STDC_FORMAT_MACROS -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement")

# Enable multi-threaded compilation
set(CMAKE_BUILD_PARALLEL_LEVEL 4)

# Options: Allow users to specify PostgreSQL installation directories
option(POSTGRES_INSTALL_DIR "PostgreSQL installation directory" "")

# Find PostgreSQL
if(POSTGRES_INSTALL_DIR)
    set(PG_INCLUDE_DIR "${POSTGRES_INSTALL_DIR}/include")
    set(PG_LIBRARY_DIR "${POSTGRES_INSTALL_DIR}/lib")
    find_path(PG_INCLUDE_DIR NAMES postgres.h PATHS ${PG_INCLUDE_DIR} NO_DEFAULT_PATH)
    find_library(PG_LIBRARY NAMES pq PATHS ${PG_LIBRARY_DIR} NO_DEFAULT_PATH)
else()
    find_path(PG_INCLUDE_DIR NAMES postgres.h)
    find_library(PG_LIBRARY NAMES pq)
endif()

# Find other dependencies
find_library(READLINE_LIBRARY NAMES readline)
find_library(LZ4_LIBRARY NAMES lz4)
find_package(Threads REQUIRED)

# Check dependencies
if(NOT PG_INCLUDE_DIR OR NOT PG_LIBRARY)
    message(FATAL_ERROR "PostgreSQL not found. Please specify POSTGRES_INSTALL_DIR.")
endif()

if(NOT READLINE_LIBRARY)
    message(FATAL_ERROR "Readline library not found.")
endif()

if(NOT LZ4_LIBRARY)
    message(FATAL_ERROR "LZ4 library not found.")
endif()

# Set directory structure
set(BASE_DIR ${CMAKE_SOURCE_DIR})
set(SRC_DIR ${BASE_DIR}/src)
set(INCL_DIR ${BASE_DIR}/incl)
set(LIB_DIR ${BASE_DIR}/lib)
set(INTERFACE_DIR ${BASE_DIR}/interfaces)
set(INTERFACE_SRC_DIR ${INTERFACE_DIR}/src)
set(INTERFACE_INCL_DIR ${INTERFACE_DIR}/incl)
set(INTERFACE_LIB_DIR ${INTERFACE_DIR}/lib)
set(PARSER_DIR ${BASE_DIR}/parser)
set(PARSER_SRC_DIR ${PARSER_DIR}/src)
set(PARSER_INCL_DIR ${PARSER_DIR}/include)
set(BIN_DIR ${BASE_DIR}/bin)

# Create output directories
file(MAKE_DIRECTORY ${LIB_DIR})
file(MAKE_DIRECTORY ${INTERFACE_LIB_DIR})

# Include directories
include_directories(
    ${INCL_DIR}
    ${PG_INCLUDE_DIR}
    ${PARSER_INCL_DIR}
    ${INTERFACE_INCL_DIR}
)

# System libraries
set(SYSTEM_LIBS
    ${CMAKE_THREAD_LIBS_INIT}
    m
    ${LZ4_LIBRARY}
)

# Recursively collect all .c files in src directory
file(GLOB_RECURSE SRC_FILES ${SRC_DIR}/*.c)

# Recursively collect all .c files in parser directory
file(GLOB_RECURSE PARSER_FILES ${PARSER_SRC_DIR}/*.c)

# Recursively collect all .c files in interfaces/src directory
file(GLOB_RECURSE INTERFACE_FILES ${INTERFACE_SRC_DIR}/*.c)

# Create a static library to include all src and parser object files
add_library(castor_static STATIC ${SRC_FILES} ${PARSER_FILES})
set_target_properties(castor_static PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${LIB_DIR})

# Create interface library
add_library(xsynch SHARED ${INTERFACE_FILES})
set_target_properties(xsynch PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${INTERFACE_LIB_DIR})
target_link_libraries(xsynch castor_static ${SYSTEM_LIBS})

# Process bin/xscsci directory
set(XSCSCI_DIR ${BIN_DIR}/xscsci)

# Generate lexer and parser files
set(XSCSCI_GRAM_Y ${XSCSCI_DIR}/xscsci_gram.y)
set(XSCSCI_GRAM_C ${XSCSCI_DIR}/xscsci_gram.c)
set(XSCSCI_GRAM_H ${XSCSCI_DIR}/xscsci_gram.h)
set(XSCSCI_SCAN_L ${XSCSCI_DIR}/xscsci_scan.l)
set(XSCSCI_SCAN_C ${XSCSCI_DIR}/xscsci_scan.c)
set(XSCSCI_PRESCAN_L ${XSCSCI_DIR}/xscsci_prescan.l)
set(XSCSCI_PRESCAN_C ${XSCSCI_DIR}/xscsci_prescan.c)

# Generate parser files
add_custom_command(
    OUTPUT ${XSCSCI_GRAM_C} ${XSCSCI_GRAM_H}
    COMMAND bison -Wno-deprecated -d -o ${XSCSCI_GRAM_C} ${XSCSCI_GRAM_Y}
    DEPENDS ${XSCSCI_GRAM_Y}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Generate lexer files
add_custom_command(
    OUTPUT ${XSCSCI_SCAN_C}
    COMMAND flex -Cfe -p -p -o${XSCSCI_SCAN_C} ${XSCSCI_SCAN_L}
    DEPENDS ${XSCSCI_SCAN_L} ${XSCSCI_GRAM_H}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_custom_command(
    OUTPUT ${XSCSCI_PRESCAN_C}
    COMMAND flex -Cfe -p -p -o${XSCSCI_PRESCAN_C} ${XSCSCI_PRESCAN_L}
    DEPENDS ${XSCSCI_PRESCAN_L}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Create a custom target to generate parser files
add_custom_target(generate_parser_files
    DEPENDS ${XSCSCI_GRAM_C} ${XSCSCI_GRAM_H} ${XSCSCI_SCAN_C} ${XSCSCI_PRESCAN_C}
)

# xscsci source files
set(XSCSCI_SOURCES
    ${XSCSCI_DIR}/xscsci.c
    ${XSCSCI_DIR}/xscsci_input.c
    ${XSCSCI_DIR}/xscsci_output.c
    ${XSCSCI_DIR}/xscsci_tabcomplete.c
    ${XSCSCI_DIR}/xscsci_precommand.c
    ${XSCSCI_DIR}/xscsci_scansup.c
    ${XSCSCI_GRAM_C}
    ${XSCSCI_PRESCAN_C}
)

# Compile xscsci
add_executable(xscsci ${XSCSCI_SOURCES})
target_link_libraries(xscsci castor_static xsynch ${READLINE_LIBRARY} ${SYSTEM_LIBS})
set_target_properties(xscsci PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${XSCSCI_DIR})

# Ensure dependencies
add_dependencies(xscsci generate_parser_files xsynch)

# Process other bin directories
set(BIN_SUBDIRS capture integrate receivepglog xmanager)
foreach(SUBDIR ${BIN_SUBDIRS})
    set(SUBDIR_DIR ${BIN_DIR}/${SUBDIR})
    
    # Collect source files
    file(GLOB SUBDIR_SOURCES ${SUBDIR_DIR}/*.c)
    
    # Compile executable
    add_executable(${SUBDIR} ${SUBDIR_SOURCES})
    target_link_libraries(${SUBDIR} castor_static ${PG_LIBRARY} ${SYSTEM_LIBS})
    set_target_properties(${SUBDIR} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${SUBDIR_DIR})
endforeach()

# Clean target
add_custom_target(clean_all
    COMMAND rm -rf ${LIB_DIR}/* ${INTERFACE_LIB_DIR}/* ${BIN_DIR}/capture/capture ${BIN_DIR}/integrate/integrate ${BIN_DIR}/receivepglog/receivelog ${BIN_DIR}/xscsci/xscsci ${BIN_DIR}/xscsci/xscsci_gram.c ${BIN_DIR}/xscsci/xscsci_gram.h ${BIN_DIR}/xscsci/xscsci_scan.c ${BIN_DIR}/xscsci/xscsci_prescan.c ${BIN_DIR}/xmanager/xmanager
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
