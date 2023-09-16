package SurfTest

const DEFAULT_META_FILENAME string = "index.db"
const DEFAULT_BLOCK_SIZE int = 4096

const META_INIT_BY_FILENAME int = 0
const META_INIT_BY_PARAMS int = 1
const META_INIT_BY_CONFIG_STR int = 2

const FILENAME_INDEX int = 0
const VERSION_INDEX int = 1
const HASH_LIST_INDEX int = 2

const CONFIG_DELIMITER string = ","
const HASH_DELIMITER string = " "

const FILE_INIT_VERSION_STR string = "1"
const FILE_INIT_VERSION int = 1
const NON_EXIST_FILE_VERSION_STR string = "0"
const NON_EXIST_FILE_VERSION int = 0
const TOMBSTONE_HASH string = "0"

const SURF_CLIENT string = "[Surfstore RPCClient]:"
const SURF_SERVER string = "[Surfstore Server]:"

const LOAD_FROM_DIR int = 0
const LOAD_FROM_METAFILE int = 1
