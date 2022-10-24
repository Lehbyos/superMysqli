<?php

namespace SuperMysqli;

/**
 *
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
        if (isset(self::$connections[$options['alias']]))
            throw new Exception("Connection alias already exists");

        $alias = $options['alias'] ?? 'default';

        self::$connections[$alias] = new DatabaseConnection($options['host'], $options['db'], $options['user'], $options['password']);
        if (count(self::$connections) == 1)
            self::$defaultConnection = $alias;
    }

    /**
     * @throws Exception
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
     */
    private function connect(string $server, string $database, string $user, string $password)
    {
        try {
            $this->mysqli = new \mysqli($server, $user, $password, $database);
        }
        catch(Exception $e){
            throw new Exception('Connection Error: ' . $e->getMessage() . "\n" . $server . ' -- ' . $database . ' - ' . $user . ' - ' . $password);
        }
        if ($this->mysqli->connect_errno)
            throw new Exception("Error connectin to MySQL/MariaDB: (" . $this->mysqli->connect_errno . ") " . $this->mysqli->connect_error);
    }

    /** @var mysqli */
    public $mysqli = null;

    public static function getInstance(string $alias = 'default'){
        if (isset(self::$connections[$alias]))
            return self::$connections[$alias];
        throw new Exception('DatabaseConnection instance with alias "' . $alias . '" not found');
    }

    // Turns autocommit on/off for transactions
    public function autoCommit($mode)
    {
        $this->mysqli->autocommit($mode);
    }

    // Commits transaction
    public function Commit()
    {
        $this->mysqli->commit();
    }

    // Rollbacks transaction
    public function Rollback()
    {
        $this->mysqli->rollback();
    }

    // Get the last insert id
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
     * Ejecuta sentencia SQL para obtener datos.
     * Este método devuelve cada fila como un OBJETO, donde cada campo de la consulta es obtenido como atributo.
     * @param string $sql SQL a ejecutar
     * @param array|null $params Parámetros requeridos por la consulta (arreglo con los valores)
     * @param array|callable|null $format Campos obtenidos de la consulta que deben ser codificados en utf8 (utf_encode), o función de formateo. Esta
     * función recibe como único parámetro la fila obtenida del SQL (convertida a objeto), y debe retornar el objeto debidamente formateado (con campos
     * codificados en UTF-8, con más campos, etc).
     * @return array Datos retornados por la consulta; puede ser array vacío si no hay datos.
     * @throws Exception Si hay problemas en la ejecución del SQL (preparación, asociación de parámtros o ejecución)
     */
    public function selectQuery(string $sql, $params = null, $format = null): array
    {
        $stmt = $this->mysqli->prepare($sql);
        if ($stmt === false)
            throw new Exception('No se pudo preparar consulta de datos: ' . $this->mysqli->error);

        if (($params !== null) && (count($params) > 0)) {
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new Exception('No se pudo asignar parámetros');
        }
        if ($stmt->execute() === false)
            throw new Exception('Error ejecutando consulta: ' . $this->mysqli->error);

        if ($format !== null){
            $formatFunction = is_callable($format);
            $toUtfFields = is_array($format);
        } else {
            $formatFunction = $toUtfFields = false;
        }

        $resp = [];
        $result = $stmt->get_result();

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
     * Retorna sólo una fila de un conjunto de resultados.
     * Por defecto, este método devolverá el primer registro obtenido del conjunto de datos; si no hay datos, el
     * método retornará <tt>null</tt>
     * @param string $sql Sentencia SQL a ejecutar para obtener datos.
     * @param array|null $params Parámetros (opcionales) de la sentencia SQL.
     * @param array|callable|null $format Opcional, formato a realizar en el objeto respuesta.
     * <ul>
     * <li>Si es un array, tendrá los nombres de campos a convertir con utf8_encode.
     * <li>Si es callable, será una función que recibe como parámetro el objeto a convertir, y retornará dicho objeto
     * con el formato que se requiera.</li>
     * </ul>
     *
     * @return object|null Primer registro del conjunto de datos, <tt>null</tt> si no hay datos de respuesta.
     * @throws Exception
     */
    public function singleRowQuery(string $sql, ?array $params = null,  $format = null): ?object
    {
        $stmt = $this->mysqli->prepare($sql);
        if ($stmt === false)
            throw new Exception('No se pudo preparar consulta de datos');

        if (($params !== null) && (count($params) > 0)) {
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new Exception('No se pudo asignar parámetros');
        }
        if ($stmt->execute() === false)
            throw new Exception('Error ejecutando consulta: ' . $this->mysqli->error);

        if ($format !== null){
            $formatFunction = is_callable($format);
            $toUtfFields = is_array($format);
        } else {
            $formatFunction = $toUtfFields = false;
        }

        $result = $stmt->get_result();
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
     * Ejecuta (y obtiene resultado) de una consulta que retorna un único valor (select de un campo, select count(), etc).
     * @param mixed $sql Sentencia SQL a ejecutar.
     * @param mixed|null $params Array de parámetros para ejecutar la consulta, <code>null</code>si no los necesita.
     * @return mixed Valor de respuesta de la consulta.
     * @throws Exception Si no puede prepararse consulta, si no puede ejecutarse.
     */
    public function singleValueQuery($sql, $params = null)
    {
        $stmt = $this->mysqli->prepare($sql);
        if ($stmt === false)
            throw new Exception('No se pudo preparar consulta de datos: ' . $this->mysqli->error);

        if (($params !== null) && (count($params) > 0)) {
            if (call_user_func_array([$stmt, 'bind_param'], $this->processParameters($params)) === false)
                throw new Exception('No se pudo asignar parámetros');
        }
        if ($stmt->execute() === false)
            throw new Exception('Error ejecutando consulta: ' . $this->mysqli->error);

        $result = $stmt->get_result();
        if ($result === false) {
            $stmt->close();
            throw new Exception('Error obteniendo resultado: ' . $stmt->error);
        }

        $row = $result->fetch_array(MYSQLI_NUM);
        if (($row !== false) && (is_array($row)))
            $resp = $row[0];
        $result->close();
        $stmt->close();
        if (!isset($resp))
            throw new Exception('Error: resultado obtenido no es válido');
        return $resp;
    }

    /**
     * Ejecuta sentencia que no entrega datos (como insert, update, delete)
     * @param mixed $sql Sentencia a ejecutar
     * @param array|null $params Parámetros requeridos por la consulta
     * @return int Número de filas afectadas.
     * @throws Exception Si hay problemas en la ejecución del SQL (preparación, asociación de parámtros o ejecución)
     */
    public function executeQuery($sql, $params = null): int
    {
        $stmt = $this->mysqli->Prepare($sql);
        if ($stmt === false)
            throw new Exception('No se pudo preparar consulta de datos: ' . $this->mysqli->error);
        if (call_user_func_array([$stmt, 'bind_param'], $this->processParameters($params)) === false)
            throw new Exception('No se pudo asignar parámetros');
        if ($stmt->execute() === false)
            throw new Exception('Error ejecutando consulta: ' . $this->mysqli->error);

        $resp = $stmt->affected_rows;
        $stmt->close();
        return $resp;
    }

    /**
     * Método para ejecución "por lote" (ejecutar varias veces la misma consulta, con datos diferentess).
     * El resultado es un array de objetos de resultado, donde cada uno de estos tiene las siguientes propiedades:
     * - $index : Índice dentro de $params donde estaba el dato
     * - $status: Estado de la ejecución; puede ser OK o ERROR.
     * - $msg   : Descripción. Si $status es OK, indica número de filas afectadas; si es ERROR, el mensaje de error entregado por la base de datos.
     * @param string $sql Sentencia SQL a ejecutar.
     * @param array $params Matriz con datos. Cada fila es un arreglo con los parámetros que requiere sentencia SQL.
     * @return array Arreglo con detalle de las respuestas.
     * @throws Exception Si no se indican parámetros o no es una matriz, y si no puede analizarse el SQL.
     */
    public function executeBatchQuery(string $sql, array $params): array
    {
        if (($params === null) || (!is_array($params)))
            throw new Exception('Ejecución en lote DEBE tener parámetros');

        $stmt = $this->mysqli->prepare($sql);
        if ($stmt === false)
            throw new Exception('No se pudo preparar consulta de datos');

        $resultados = [];

        foreach ($params as $index => $ejecuciones) {
            $res = new stdClass;
            $res->index = $index;
            $res->status = '';
            $res->msg = '';

            if (call_user_func_array([$stmt, 'bind_param'], $this->processParameters($ejecuciones)) === false) {
                $res->status = 'ERROR';
                $res->msg = 'Error en binding de parámetros: ' . $stmt->error;
            } else {
                $filas = $stmt->execute();
                if ($filas === false) {
                    $res->status = 'ERROR';
                    $res->msg = 'Error de ejecución: ' . $stmt->error;
                } else {
                    $res->status = 'OK';
                    $res->msg = true;
                }
            }
            $resultados[] = $res;
        }

        $stmt->close();
        return $resultados;
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
     * @param string $name
     * @param array|null $params
     * @param array|callable|null $format
     * @return array
     * @throws Exception
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

        $stmt = $this->mysqli->prepare($sql);
        if ($stmt->prepare($sql) === false)
            throw new Exception('No se pudo preparar consulta de datos: ' . $stmt->error);

        if (($params !== null) && (count($params) > 0)) {
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new Exception('No se pudo asignar parámetros');
        }

        if ($stmt->execute() === false)
            throw new Exception("Error ejecutando procedimiento $name: " . $stmt->error);

        $resp = [];
        $result = $stmt->get_result();

        if ($format !== null){
            $formatFunction = is_callable($format);
            $toUtfFields = is_array($format);
        } else {
            $formatFunction = $toUtfFields = false;
        }

        while ($row = $result->fetch_array(MYSQLI_NUM)) {
            if ($formatFunction) {
                $resp[] = $format($row);
            } else if ($toUtfFields) {
                foreach ($format as $field) {
                    $row->$field = utf8_encode($row[$field]);
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
            throw new Exception('No se pudo preparar consulta de datos: ' . $stmt->error);

        if (($params !== null) && (count($params) > 0)) {
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new Exception('No se pudo asignar parámetros');
        }

        if ($stmt->execute() === false)
            throw new Exception("Error ejecutando procedimiento $sp_name: " . $stmt->error);

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
            throw new Exception("Error obteniendo resultado(1) de procedimiento $sp_name: " . $stmt->error);

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
            throw new Exception('No se pudo preparar consulta de datos: ' . $stmt->error);

        if (($params !== null) && (count($params) > 0)) {
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new Exception('No se pudo asignar parámetros');
        }

        if ($stmt->execute() === false)
            throw new Exception("Error ejecutando procedimiento $sp_name: " . $stmt->error);

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
            throw new Exception("Error obteniendo resultado(1) de procedimiento $sp_name: " . $stmt->error);

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
            throw new Exception('No se pudo preparar consulta de datos: ' . $this->mysqli->error);

        if (($params !== null) && (count($params) > 0)){
            $queryParameters = $this->processParameters($params);
            if (call_user_func_array([$stmt, 'bind_param'], $queryParameters) === false)
                throw new Exception('No se pudo asignar parámetros');
        }
        if ($stmt->execute() === false)
            throw new Exception('Error ejecutando consulta: ' . $this->mysqli->error);

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