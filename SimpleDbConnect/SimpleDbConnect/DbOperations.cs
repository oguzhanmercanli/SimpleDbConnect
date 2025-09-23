using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SimpleDbConnect
{
    public class DbOperations
    {
        private readonly string _connectionString = "";
        private readonly int _executionTimeout;

        /// <summary>
        /// Initializes a new instance of DbOperations with a given connection string.
        /// Default execution timeout is 30 seconds.
        /// </summary>
        public DbOperations(string connectionString)
        {
            this._connectionString = connectionString;
            this._executionTimeout = 30;
        }



        /// <summary>
        /// Initializes a new instance of DbOperations with a given connection string
        /// and a custom execution timeout (in seconds).
        /// </summary>
        public DbOperations(string connectionString, int executionTimeout)
        {
            this._connectionString = connectionString;
            this._executionTimeout = executionTimeout;
        }

        /// <summary>
        /// Tests the database connection.
        /// Returns "Success" if the connection is established, otherwise returns the error message.
        /// </summary>
        public string CheckConnection()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            return "Success";
        }


        /// <summary>
        /// Creates and returns a new SqlConnection.
        /// Throws an exception with the provided message if an error occurs.
        /// </summary>
        public SqlConnection GetSqlConnection(string message)
        {            
            try
            {
                return new SqlConnection(_connectionString);
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a non-query SQL command (INSERT, UPDATE, DELETE)
        /// and returns the number of affected rows.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public int ExecuteQuery(string query, string message)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.Clear();
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.CommandType = CommandType.Text;

                        connection.Open();
                        return cmd.ExecuteNonQuery();
                    }
                }                
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }
        }

        /// <summary>
        /// Executes a non-query SQL command using an existing open connection.
        /// Returns the number of affected rows.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public int ExecuteQuery(SqlConnection connection, string query, string message)
        {            
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.Parameters.Clear();
                    cmd.CommandType = CommandType.Text;
                    return (int)cmd.ExecuteNonQuery();
                }                   
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }


        /// <summary>
        /// Executes a non-query SQL command with parameters
        /// and returns the number of affected rows.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public int ExecuteQueryWithParameters(string query, Dictionary<string, object> parameters, string message)
        {            
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, connection))
                    {                        
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.Parameters.Clear();
                        foreach (KeyValuePair<string, object> item in parameters)
                        {
                            cmd.Parameters.AddWithValue(item.Key, item.Value);
                        }
                        cmd.CommandType = CommandType.Text;                        
                        connection.Open();                        
                        return (int)cmd.ExecuteNonQuery();
                    }
                }              
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a non-query SQL command with parameters using an existing open connection.
        /// Returns the number of affected rows.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public int ExecuteQueryWithParameters(SqlConnection connection, string query, Dictionary<string, object> parameters, string message)
        {            
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.Parameters.Clear();
                    foreach (KeyValuePair<string, object> item in parameters)
                    {
                        cmd.Parameters.AddWithValue(item.Key, item.Value);
                    }
                    cmd.CommandType = CommandType.Text;
                    return (int)cmd.ExecuteNonQuery();
                }                    
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a scalar query and returns the first column of the first row in the result set.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public object ExecuteScalarQuery(string query, string message)
        {            
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, connection))
                    {                        
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.Parameters.Clear();
                        cmd.CommandType = CommandType.Text;                        
                        connection.Open();                        
                        return (object)cmd.ExecuteScalar();
                    }
                }               
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a scalar query using an existing open connection
        /// and returns the first column of the first row in the result set.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public object ExecuteScalarQuery(SqlConnection connection, string query, string message)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.Parameters.Clear();
                    cmd.CommandType = CommandType.Text;
                    return (object)cmd.ExecuteScalar();
                }                    
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }
        }

        /// <summary>
        /// Executes a scalar query with parameters and returns the first column of the first row.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public object ExecuteScalarQueryWithParameters(string query, Dictionary<string, object> parameters, string message)
        {            
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, connection))
                    {
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.Parameters.Clear();
                        foreach (KeyValuePair<string, object> item in parameters)
                        {
                            cmd.Parameters.AddWithValue(item.Key, item.Value);
                        }
                        cmd.CommandType = CommandType.Text;                        
                        connection.Open();                        
                        return (object)cmd.ExecuteScalar();
                    }
                }               
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a scalar query with parameters using an existing open connection
        /// and returns the first column of the first row.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public object ExecuteScalarQueryWithParameters(SqlConnection connection, string query, Dictionary<string, object> parameters, string message)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.Parameters.Clear();
                    foreach (KeyValuePair<string, object> item in parameters)
                    {
                        cmd.Parameters.AddWithValue(item.Key, item.Value);
                    }
                    cmd.CommandType = CommandType.Text;
                    return (object)cmd.ExecuteScalar();
                }                    
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }
        }

        /// <summary>
        /// Executes a query with parameters and returns the result as a DataTable.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public DataTable GetDataTableWithParameters(string query, Dictionary<string, object> parameters, string message)
        {
            DataTable dt = new DataTable();            
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, connection))
                    {
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.Parameters.Clear();
                        foreach (KeyValuePair<string, object> item in parameters)
                        {
                            cmd.Parameters.AddWithValue(item.Key, item.Value);
                        }                        
                        connection.Open();                        
                        SqlDataAdapter da = new SqlDataAdapter(cmd);                        
                        da.Fill(dt);
                    }
                }                
            }
            catch (Exception ex)
            {

                throw new Exception(message + " Details = " + ex.Message);
            }            
            return dt;
        }

        /// <summary>
        /// Executes a query with parameters using an existing open connection
        /// and returns the result as a DataTable.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public DataTable GetDataTableWithParameters(SqlConnection connection, string query, Dictionary<string, object> parameters, string message)
        {
            DataTable dt = new DataTable();
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.Parameters.Clear();
                    foreach (KeyValuePair<string, object> item in parameters)
                    {
                        cmd.Parameters.AddWithValue(item.Key, item.Value);
                    }
                    SqlDataAdapter da = new SqlDataAdapter(cmd);                    
                    da.Fill(dt);
                }                    
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }

            return dt;
        }

        /// <summary>
        /// Executes a query and returns the result as a DataTable.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public DataTable GetDataTable(string query, string message)
        {
            DataTable dt = new DataTable();
            
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, connection))
                    {                        
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.Parameters.Clear();                        
                        connection.Open();                        
                        SqlDataAdapter da = new SqlDataAdapter(cmd);                        
                        da.Fill(dt);
                    }
                }                     
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }           
            return dt;
        }

        /// <summary>
        /// Executes a query using an existing open connection
        /// and returns the result as a DataTable.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public DataTable GetDataTable(SqlConnection connection, string query, string message)
        {
            DataTable dt = new DataTable();
            try
            {

                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.Parameters.Clear();

                    SqlDataAdapter da = new SqlDataAdapter(cmd);                    
                    da.Fill(dt);
                }                    
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }

            return dt;
        }

        /// <summary>
        /// Executes a SQL command within a transaction.
        /// Commits if successful, otherwise rolls back.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteTransaction(string query, string message)
        {            
            SqlTransaction transaction = null;
            string transactionName = "OM_Transaction";
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, connection))
                    {
                        try
                        {
                            connection.Open();
                            transaction = connection.BeginTransaction(transactionName);
                            cmd.Parameters.Clear();
                            cmd.CommandTimeout = _executionTimeout;
                            cmd.CommandType = CommandType.Text;
                            cmd.Transaction = transaction;

                            cmd.ExecuteNonQuery();
                            transaction.Commit();
                        }
                        catch (Exception)
                        {
                            try
                            {
                                if (transaction != null && transaction.Connection != null)
                                {
                                    if (transaction.Connection.State == ConnectionState.Open)
                                    {
                                        transaction.Rollback(transactionName);
                                    }
                                }
                            }
                            catch (Exception innerException)
                            {
                                throw new Exception("Rollback error = " + innerException.Message);
                            }
                        }
                    }                    
                }
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a SQL command within a transaction using an existing connection.
        /// Commits if successful, otherwise rolls back.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteTransaction(SqlConnection connection, string query, string message)
        {
            SqlTransaction transaction = null;
            string transactionName = "OM_Transaction";
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    transaction = connection.BeginTransaction(transactionName);
                    cmd.Parameters.Clear();
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.CommandType = CommandType.Text;
                    cmd.Transaction = transaction;

                    cmd.ExecuteNonQuery();
                    transaction.Commit();
                }                                
            }
            catch (Exception ex)
            {
                try
                {
                    if (transaction != null && transaction.Connection != null)
                    {
                        if (transaction.Connection.State == ConnectionState.Open)
                        {
                            transaction.Rollback(transactionName);
                        }
                    }
                }
                catch (Exception innerException)
                {
                    throw new Exception("Rollback error = " + innerException.Message);
                }
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a SQL command with parameters within a transaction.
        /// Commits if successful, otherwise rolls back.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteTransactionWithParameters(string query, Dictionary<string, object> parameters, string message)
        {            
            SqlTransaction transaction = null;
            string transactionName = "OM_Transaction";
            try
            {

                using(SqlConnection connection = new SqlConnection(_connectionString))
                {
                    try
                    {
                        using (SqlCommand cmd = new SqlCommand(query, connection))
                        {
                            connection.Open();
                            transaction = connection.BeginTransaction(transactionName);
                            cmd.Parameters.Clear();
                            foreach (KeyValuePair<string, object> item in parameters)
                            {
                                cmd.Parameters.AddWithValue(item.Key, item.Value);
                            }
                            cmd.CommandTimeout = _executionTimeout;
                            cmd.CommandType = CommandType.Text;
                            cmd.Transaction = transaction;

                            cmd.ExecuteNonQuery();
                            transaction.Commit();
                        }                            
                    }
                    catch (Exception)
                    {
                        try
                        {
                            if (transaction != null && transaction.Connection != null)
                            {
                                if (transaction.Connection.State == ConnectionState.Open)
                                {
                                    transaction.Rollback(transactionName);
                                }
                            }
                        }
                        catch (Exception innerException)
                        {
                            throw new Exception("Rollback işleminde hata oluştu = " + innerException.Message);
                        }
                        throw;
                    }

                }
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }

        /// <summary>
        /// Executes a SQL command with parameters within a transaction using an existing connection.
        /// Commits if successful, otherwise rolls back.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteTransactionWithParameters(SqlConnection connection, string query, Dictionary<string, object> parameters, string message)
        {
            SqlTransaction transaction = null;
            string transactionName = "OM_Transaction";            
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    transaction = connection.BeginTransaction(transactionName);
                    cmd.Parameters.Clear();
                    foreach (KeyValuePair<string, object> item in parameters)
                    {
                        cmd.Parameters.AddWithValue(item.Key, item.Value);
                    }
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.CommandType = CommandType.Text;
                    cmd.Transaction = transaction;
                    cmd.ExecuteNonQuery();
                    transaction.Commit();
                }
            }
            catch (Exception ex)
            {
                try
                {
                    if (transaction != null && transaction.Connection != null)
                    {
                        if (transaction.Connection.State == ConnectionState.Open)
                        {
                            transaction.Rollback(transactionName);
                        }
                    }
                }
                catch (Exception innerException)
                {
                    throw new Exception("Rollback işleminde hata oluştu = " + innerException.Message);
                }
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }


        /// <summary>
        /// Executes the specified stored procedure without parameters.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteStoredProcedure(string procedure, string message)
        {            
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(procedure, connection))
                    {
                        connection.Open();
                        cmd.Parameters.Clear();
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.ExecuteNonQuery();
                    }                        
                }
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }
        }

        /// <summary>
        /// Executes the specified stored procedure without parameters
        /// using an existing SqlConnection. The method does not handle connection closing.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteStoredProcedure(SqlConnection connection, string procedure, string message)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(procedure, connection))
                {
                    cmd.Parameters.Clear();
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.ExecuteNonQuery();
                }                    
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }
        }

        /// <summary>
        /// Executes the specified stored procedure with parameters.
        /// The method manages the connection lifecycle (open/close).
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteStoredProcedureWithParameters(string procedure, Dictionary<string, object> parameters, string message)
        {
            
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(procedure, connection))
                    {
                        connection.Open();
                        cmd.Parameters.Clear();
                        foreach (KeyValuePair<string, object> item in parameters)
                        {
                            cmd.Parameters.AddWithValue(item.Key, item.Value);
                        }
                        cmd.CommandTimeout = _executionTimeout;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.ExecuteNonQuery();
                    }                        
                }
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }            
        }


        /// <summary>
        /// Executes the specified stored procedure with parameters
        /// using an existing SqlConnection. The method does not handle connection closing.
        /// Throws an exception with the given message if an error occurs.
        /// </summary>
        public void ExecuteStoredProcedureWithParameters(SqlConnection connection, string procedure, Dictionary<string, object> parameters, string message)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(procedure, connection))
                {
                    cmd.Parameters.Clear();
                    foreach (KeyValuePair<string, object> item in parameters)
                    {
                        cmd.Parameters.AddWithValue(item.Key, item.Value);
                    }
                    cmd.CommandTimeout = _executionTimeout;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.ExecuteNonQuery();
                }                    
            }
            catch (Exception ex)
            {
                throw new Exception(message + " Details = " + ex.Message);
            }
        }        
    }
}
