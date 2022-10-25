<?php

namespace SuperMysqli;

use http\Exception\InvalidArgumentException;
use SuperMysqli\Exceptions;

/**
 * Wrapper and enhancer to a mysqli database connection.
 * This class also acts like a kind of connection factory, allowing to manage
 * multiple of them, identified by an alias.
 */
class DatabaseConnection
{
    /**
     * @var array Configured connections
     */
    private static $connections = [];

    /**
     * @var string Name of the default connection
     */
    private static $defaultConnection = 'default';

    /**
     * Add a new database configuration
     * The spected keys for the options array are:
     * <ul>
     * <li><b>host</b> Server name/IP address of the server</li>
     * <li><b>db</b> The database name</li>
     * <li><b>user</b> User to connect to the database</li>
     * <li><b>password</b> The password to the given user</li>
     * <li><b>alias</b> The alias to use for the connection. If no one is given, <tt>default</tt> is used.
     * If just ONE connection is configured, that connection will be the default one</li>
     * </ul>
     * @param array $options Connections options
     * @return void
     */
    public static function addConnection(array $options){
        //string $host, string $dbName, string $user, string $password, string $alias = 'default'
        $alias = $options['alias'] ?? 'default';

        if (isset(self::$connections[$options['alias']]))
            throw new Exceptions\AliasAlreadyDefinedException($alias);

        self::$connections[$alias] = new DatabaseConnection($options['host'], $options['db'], $options['user'], $options['password']);
        if (count(self::$connections) == 1)
            self::$defaultConnection = $alias;
    }

    /**
     * Initialize a new connection
     * @throws Exceptions\ConnectionException
     */
    private function __construct(string $host, string $dbName, string $user, string $password){
        $this->connect($host, $dbName, $user, $password);
    }

    /**
     * Create and open a connection to a database.
     * @param string $server Database server
     * @param string $database Database name
     * @param string $user User to log in into the server
     * @param string $password Password for the user
     * @return void
     * @throws Exceptions\ConnectionException If connection cannot be established with the database
     */
    private function connect(string $server, string $database, string $user, string $password)
    {
        try {
            $this->mysqli = new \mysqli($server, $user, $password, $database);
        }
        catch(Exception $e){
            throw new Exceptions\ConnectionException($server, $user, $password, $database, $e->getMessage());
        }
        if ($this->mysqli->connect_errno)
            throw new Exceptions\ConnectionException($server, $user, $password, $database, $this->mysqli->connect_error);
    }

    /** @var \mysqli mysqli connection to the database  */
    private $mysqli = null;

    /**
     * Gets the database connection with the given alias.
     * @param string $alias Alias of the connection. No alias is required if there is only one configured connection
     * @return DatabaseConnection Get connection instance.
     * @throws \Exception If there is no configured connection with the given alias
     */
    public static function getInstance(string $alias = ''): ?DatabaseConnection{
        $dbAlias = ($alias == '') ? self::$defaultConnection : $alias;
        if (isset($dbAlias))
            return self::$connections[$dbAlias];
        throw new Exceptions\AliasNotFoundException($dbAlias);
    }

    /**
     * Sets the autocommit mode.
     * Set this to <tt>true</tt> begins a new transaction that can be confirmed with {@link commit()} or canceled with {@link rollback()}
     * @param bool $state Autocommit state
     * @return void
     */
    public function autoCommit(bool $state = true): void
    {
        $this->mysqli->autocommit($state);
    }

    /**
     * Confirm/Commit a started transaction.
     * Once commited, the autoCommit state will be set to <tt>false</tt>
     * @return void
     */
    public function commit()
    {
        $this->mysqli->commit();
        $this->mysqli->autocommit(false);
    }

    /**
     * Cancel/Rollback a started transaction.
     * Once rolled back, the autoCommit state will be set to <tt>false</tt>
     * @return void
     */
    public function rollback()
    {
        $this->mysqli->rollback();
        $this->mysqli->autocommit(false);
    }

    /**
     * Gets the last generated value for an AUTO_INCREMENT column by the last query
     * @return mixed The value of the AUTO_INCREMENT field that was updated by the previous query.
     * Returns zero if there was no previous query on the connection or if the query did not update an AUTO_INCREMENT value
     */
    public function lastID()
    {
        return $this->mysqli->insert_id;
    }

    /**
     * Process received parameters, mapping it to the required array by the mysqli's bind_param() function.
     * @param array|null $params Array of parameters to process
     * @return array
     */
    protected function processParameters(?array $params): array
    {
        if (($params === null) || (count($params) == 0))
            return [];
        $values = [0 => ''];
        foreach ($params as $key => $param) {
            if ($param === null){
                $values[0] .= 'i';
                $params[$key] = null;
            } else {
                if (is_int($param)){
                    $values[0] .= 'i';
                } else if (is_float($param)){
                    $values[0] .= 'd';
                } else {
                    $values[0] .= 's';
                    $params[$key] = $this->mysqli->real_escape_string(trim($param));
                }
            }
            $values[] = &$params[$key];
        }
        return $values;
    }

    /**
     * Prepares and bind the parameters of a SQL statement.
     * @param string $sql The SQL statement to execute
     * @param array|null $params Query parameters
     * @return \mysqli_stmt Prepared statement, with bound parameters (if any), ready to be executed.
     * @throws Exceptions\BindingParameterException
     * @throws Exceptions\PreparationException
     */
    protected function createPreparedStatement(string $sql, ?array $params = null): \mysqli_stmt{
        $stmt = $this->mysqli->prepare($sql);
        if ($stmt === false)
            throw new Exceptions\PreparationException($this->mysqli->error, $this->mysqli->errno);

        if (($params !== null) && (count($params) > 0)) {
            if (call_user_func_array([$stmt, 'bind_param'], $this->processParameters($params)) === false)
                throw new Exceptions\BindingParameterException($this->mysqli->error, $this->mysqli->errno);
        }
        return $stmt;
    }

    /**
     * Executes an SQL statement returning data.
     * Every data row will be returned as an <b>object</b>, where each attribute is a data field.
     * @param string $sql SQL Statement
     * @param array|null $params SQL statement parameters (if any)
     * @param array|callable|null $format Format for each data row.
     * <ul>
     * <li>If it is an array, contains a list of data columns that will be {@link utf8_encode()}'d.</li>
     * <li>If a callable, it will receive the data row object and return the formatted version of it. No return data will generate empty results.</li>
     * </ul>
     * @return array Data returned by the query. Empty if there is no data.
     * @throws \Exception If SQL cannot be prepared or executed.
     */
    public function selectQuery(string $sql, ?array $params = null, $format = null): array
    {
        $stmt = $this->createPreparedStatement($sql, $params);

        if ($stmt->execute() === false)
            throw new Exceptions\ExecuteStatementException($this->mysqli->error, $this->mysqli->errno);

        if ($format !== null){
            $formatFunction = is_callable($format);
            $toUtfFields = is_array($format);
        } else {
            $formatFunction = $toUtfFields = false;
        }

        $resp = [];
        $result = $stmt->get_result();
        if ($result === false)
            throw new Exceptions\NoResultException($this->mysqli->error, $this->mysqli->errno);

        while ($row = $result->fetch_object()) {
            if ($formatFunction) {
                $resp[] = $format($row);
            } else if ($toUtfFields) {
                foreach ($format as $field) {
                    $row->$field = utf8_encode($row->$field);
                }
                $resp[] = $row;
            } else
                $resp[] = $row;
        }
        $result->close();
        $stmt->close();
        return $resp;
    }

    /**
     * Returns one result row of a SQL query.
     * If the given query return more than one result, this method will return the first data row, and return <tt>null</tt> if
     * there is no data.
     * As in {@link selectQuery()}, the data row will be converted to an object.
     * @param string $sql SQL Statement.
     * @param array|null $params SQL statement parameters.
     * @param array|callable|null $format Format for the data row, as in {@link selectQuery}
     * @return object|null The data result, <tt>null</tt> if there is no data.
     * @throws Exceptions\BindingParameterException
     * @throws Exceptions\PreparationException
     * @throws Exceptions\ExecuteStatementException
     * @throws Exceptions\NoResultException
     */
    public function singleRowQuery(string $sql, ?array $params = null, $format = null): ?object
    {
        $stmt = $this->createPreparedStatement($sql, $params);
        if ($stmt->execute() === false)
            throw new Exceptions\ExecuteStatementException($this->mysqli->error, $this->mysqli->errno);

        if ($format !== null){
            $formatFunction = is_callable($format);
            $toUtfFields = is_array($format);
        } else {
            $formatFunction = $toUtfFields = false;
        }

        $result = $stmt->get_result();
        if ($result === false)
            throw new Exceptions\NoResultException($this->mysqli->error, $this->mysqli->errno);

        $resp = $result->fetch_object();
        $result->close();
        $stmt->close();

        if ($resp !== null) {
            if ($formatFunction) {
                $resp = $format($resp);
            } else if ($toUtfFields) {
                foreach ($format as $field) {
                    $resp->$field = utf8_encode($resp->$field);
                }
            }
        }

        return $resp;
    }

    /**
     * Get data from a query with return a single value instead of multiple columns and rows.
     * This method gets the value of the first column of the first row of data. If the query gets more columns/rows,
     * they will be ignored.
     * @param string $sql SQL Statement
     * @param array|null $params Params required by the query (if any)
     * @return mixed The value returned by the SQL statement.
     * @throws Exceptions\BindingParameterException
     * @throws Exceptions\PreparationException
     */
    public function singleValueQuery(string $sql, array $params = null): mixed
    {
        $stmt = $this->createPreparedStatement($sql, $params);
        if ($stmt->execute() === false)
            throw new Exceptions\ExecuteStatementException($this->mysqli->error, $this->mysqli->errno);

        $result = $stmt->get_result();
        if ($result === false) {
            $stmt->close();
            throw new Exceptions\NoResultException($this->mysqli->error, $this->mysqli->errno);
        }

        $row = $result->fetch_array(MYSQLI_NUM);
        if (($row !== false) && (is_array($row)))
            $resp = $row[0];
        $result->close();
        $stmt->close();
        if (!isset($resp))
            throw new Exceptions\InvalidResultException($this->mysqli->error, $this->mysqli->errno);

        return $resp;
    }

    /**
     * Executes a query.
     * This method is designed for queries that not return data, but the number of rows that it affected.
     * @param string $sql SQL statement.
     * @param array|null $params Parameters for the statement (if any)
     * @return int Affected rows
     * @throws Exceptions\BindingParameterException
     * @throws Exceptions\ExecuteStatementException
     * @throws Exceptions\PreparationException
     */
    public function executeQuery(string $sql, array $params = null): int
    {
        $stmt = $this->createPreparedStatement($sql, $params);
        if ($stmt->execute() === false)
            throw new Exceptions\ExecuteStatementException($this->mysqli->error, $this->mysqli->errno);

        $resp = $stmt->affected_rows;
        $stmt->close();
        return $resp;
    }

    /**
     * Call a stored procedure.
     * The procedure must return data (do SELECT queries), returning that data.
     * @param string $name Name of the procedure to call
     * @param array|null $params Parameters to the procedure
     * @param array|callable|null $format Format for the returned data (like in {@link selectQuery})
     * @return array Data obtained from the database.
     * @throws Exceptions\BindingParameterException
     * @throws Exceptions\ExecuteStatementException
     * @throws Exceptions\PreparationException
     */
    public function callStoredProcedure(string $name, ?array $params = null, $format = null): array{
        $sql = 'call ' . $name . '(';
        if ($params !== null) {
            $last = count($params) - 1;
            foreach ($params as $index => $param) {
                $sql .= '?';
                if ($index < $last)
                    $sql .= ', ';
            }
        }
        $sql .= ')';

        $stmt = $this->createPreparedStatement($sql, $params);
        if ($stmt->execute() === false)
            throw new Exceptions\ExecuteStatementException('Error executing stored procedure: ' . $this->mysqli->error, $this->mysqli->errno);

        $resp = [];
        $result = $stmt->get_result();

        if ($format !== null){
            $formatFunction = is_callable($format);
            $toUtfFields = is_array($format);
        } else {
            $formatFunction = $toUtfFields = false;
        }

        while ($row = $result->fetch_object()) {
            if ($formatFunction) {
                $resp[] = $format($row);
            } else if ($toUtfFields) {
                foreach ($format as $field) {
                    $row->$field = utf8_encode($row->$field);
                }
                $resp[] = $row;
            } else
                $resp[] = $row;
        }

        $result->close();
        $stmt->close();

        return $resp;
    }

    /**
     * Método que concentra operaciones para dar respuesta a un Datatable con server render.
     * En este caso, la respuesta más básica que se envía a DataTable debe tener la siguiente estructura:
     * - $draw: Valor que indica correlativo de solicitud; es de manejo interno de datatable.
     * - $data: Los datos a dibujar en la tabla.
     * - $recordsFiltered: Cantidad de registros obtenidos con los filtros (si los hay)
     * - $recordsTotal: La cantidad total de registros en la tabla/fuente de datos.
     * Esta implementación asume que en la consulta que trae los datos ($sql) se está usando el atributo SQL_CALC_FOUND_ROWS, lo que
     * permite obtener de forma rápida la cantidad de filas obtenidas.
     * @param mixed $draw Parámetro entregado por DataTables cuando hace el request
     * @param mixed $sql Sentencia SQL a ejecutar para obtener datos.
     * @param mixed $sqlTotal SQL que calcula el total de registros disponibles para la tabla.
     * @param mixed|null $paramsSql Parámetros para la sentencia SQL que obtiene datos.
     * @param mixed|null $paramsSqlTotal Parámetros para sentencia SQL que obtiene el total de registros.
     * @param array|callable|null $format Función que formatea cada fila de datos, o arreglo con nombres de atributos que deben ser codificados a UTF-8.
     * @return stdClass Objeto para DataTable.
     * @throws Exception
     */
    public function selectDatatableQuery($draw, $sql, $sqlTotal, $paramsSql = null, $paramsSqlTotal = null, $format = null): stdClass
    {
        $resp = new stdClass;
        $resp->draw = $draw;
        $resp->data = $this->selectQuery($sql, $paramsSql, $format);
        $resp->recordsFiltered = $this->singleValueQuery('select FOUND_ROWS()');
        $resp->recordsTotal = $this->singleValueQuery($sqlTotal, $paramsSqlTotal);
        return $resp;
    }



    /**
     * Obtiene objeto de respuesta para DataTables, obteniendo todos los datos desde un procedimiento almacenado.
     * @param int $draw Correlativo de llamada (es usado por datatables)
     * @param string $sp_name Nombre (sin parámetros) del procedimiento
     * @param array|null $params Parámetros requeridos para ejecutar el procedimiento.
     * @param array|callable|null $format Formateo del resultado. Si es array de strings, converte cada campo indicado en arreglo con utf8_encode.
     * Si es una función, esta será llamada por cada fila de resultado (la que es pasada a la función), y se espera como respuesta de la misma la
     * fila de datos formateada (notar que incluso se puede cambiar la cantidad de columnas en la respuesta, si hace falta).
     * @return stdClass Objeto con los atributos requeridos por Datatable para desplegar datos.
     * @throws Exception Si hay problemas ejecutando procedimiento.
     */
    public function getDatatableInfoFromSP(int $draw, string $sp_name, $params = null, $format = null): stdClass
    {
        $sql = 'call ' . $sp_name . '(';
        if ($params !== null) {
            $last = count($params) - 1;
            foreach ($params as $index => $param) {
                $sql .= '?';
                if ($index < $last)
                    $sql .= ', ';
            }
        }
        $sql .= ')';

        $stmt = $this->mysqli->prepare($sql);
        if ($stmt->prepare($sql) === false)
            throw new \Exception('No se pudo preparar consulta de datos: ' . $stmt->error);

        if (($params !== null) && (count($params) > 0)) {
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new \Exception('No se pudo asignar parámetros');
        }

        if ($stmt->execute() === false)
            throw new \Exception("Error ejecutando procedimiento $sp_name: " . $stmt->error);

        //ojo
        $resp = new stdClass();
        $resp->draw = $draw;
        $resp->data = [];
        $resp->recordsFiltered = null;
        $resp->recordsTotal = 0;

        $formatFunction = (($format !== null) && is_callable($format));
        $toUtfFields = (($format !== null) && is_array($format));
        $result = $stmt->get_result();

        if ($result === false)
            throw new \Exception("Error obteniendo resultado(1) de procedimiento $sp_name: " . $stmt->error);

        while ($row = $result->fetch_array(MYSQLI_NUM)) {
            if ($formatFunction) {
                $resp->data[] = $format($row);
            } else if ($toUtfFields) {
                foreach ($format as $field) {
                    $row->$field = utf8_encode($row[$field]);
                }
                $resp->data[] = $row;
            } else
                $resp->data[] = $row;
        }

        $stmt->next_result();
        $result = $stmt->get_result();
        $f = $result->fetch_array(MYSQLI_NUM);
        $resp->recordsFiltered = $f[0];

        $stmt->next_result();
        $result = $stmt->get_result();
        $f = $result->fetch_array(MYSQLI_NUM);
        $resp->recordsTotal = $f[0];

        return $resp;
    }

    /**
     * Obtiene objeto de respuesta para DataTables, obteniendo todos los datos desde un procedimiento almacenado.
     * A diferencia de {@see getDataTableInfoFromSP}, la data de respuesta es un arreglo de <b>objetos</b>, y no una
     * matriz.
     * @param int $draw Correlativo de llamada (es usado por Datatable)
     * @param string $sp_name Nombre (sin parámetros) del procedimiento
     * @param array|null $params Parámetros requeridos para ejecutar el procedimiento.
     * @param array|callable|null $format Formateo del resultado. Si es array de strings, convierte cada campo indicado en arreglo con utf8_encode.
     * Si es una función, esta será llamada por cada objeto de resultado (que es el parámetro de esta función), y se espera como respuesta de la misma la
     * fila de datos formateada (notar que incluso se puede cambiar la cantidad de columnas en la respuesta, si hace falta).
     * @return stdClass Objeto con los atributos requeridos por Datatable para desplegar datos.
     * @throws Exception Si hay problemas ejecutando procedimiento.
     */
    public function getDatatableInfoFromSPObject(int $draw, string $sp_name, array $params = null, $format = null): object
    {
        $sql = 'call ' . $sp_name . '(';
        if ($params !== null) {
            $last = count($params) - 1;
            foreach ($params as $index => $param) {
                $sql .= '?';
                if ($index < $last)
                    $sql .= ', ';
            }
        }
        $sql .= ')';

        $stmt = $this->mysqli->prepare($sql);
        if ($stmt->prepare($sql) === false)
            throw new \Exception('No se pudo preparar consulta de datos: ' . $stmt->error);

        if (($params !== null) && (count($params) > 0)) {
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new \Exception('No se pudo asignar parámetros');
        }

        if ($stmt->execute() === false)
            throw new \Exception("Error ejecutando procedimiento $sp_name: " . $stmt->error);

        //ojo
        $resp = new stdClass();
        $resp->draw = $draw;
        $resp->data = [];
        $resp->recordsFiltered = null;
        $resp->recordsTotal = 0;

        $formatFunction = (($format !== null) && is_callable($format));
        $toUtfFields = (($format !== null) && is_array($format));
        $result = $stmt->get_result();

        if ($result === false)
            throw new \Exception("Error obteniendo resultado(1) de procedimiento $sp_name: " . $stmt->error);

        while ($row = $result->fetch_object()) {
            if ($formatFunction) {
                $resp->data[] = $format($row);
            } else if ($toUtfFields) {
                foreach ($format as $field) {
                    $row->$field = utf8_encode($row->$field);
                }
                $resp->data[] = $row;
            } else
                $resp->data[] = $row;
        }

        $stmt->next_result();
        $result = $stmt->get_result();
        $f = $result->fetch_array(MYSQLI_NUM);
        $resp->recordsFiltered = $f[0];

        $stmt->next_result();
        $result = $stmt->get_result();
        $f = $result->fetch_array(MYSQLI_NUM);
        $resp->recordsTotal = $f[0];

        return $resp;
    }

    /**
     * Obtiene la única columna de resultados como un arreglo de valores
     * @param $sql
     * @param array|null $params
     * @return array
     * @throws Exception
     */
    public function selectAsArray($sql, array $params = null): array
    {
        $stmt = $this->mysqli->prepare($sql);
        if ($stmt === false)
            throw new \Exception('No se pudo preparar consulta de datos: ' . $this->mysqli->error);

        if (($params !== null) && (count($params) > 0)){
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new \Exception('No se pudo asignar parámetros');
        }
        if ($stmt->execute() === false)
            throw new \Exception('Error ejecutando consulta: ' . $this->mysqli->error);

        $resp = [];
        $result = $stmt->get_result();

        while($row = $result->fetch_row()){
            $resp[] = $row[0];
        }
        $result->close();
        $stmt->close();
        return $resp;
    }
}