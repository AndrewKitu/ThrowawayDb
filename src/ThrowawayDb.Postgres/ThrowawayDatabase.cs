using System;
using System.Diagnostics;
using Npgsql;

namespace ThrowawayDb.Postgres
{
    public class ThrowawayDatabase : IDisposable
    {
        private const int DefaultDatabasePort = 5432;
        private const string DefaultDatabaseNamePrefix = "throwawaydb";
        private readonly string originalConnectionString;
        private bool databaseCreated;

        private ThrowawayDatabase(string originalConnectionString, string databaseNamePrefix)
        {
            // Default constructor is private
            this.originalConnectionString = originalConnectionString;
            var (derivedConnectionString, databaseName) = DeriveThrowawayConnectionString(originalConnectionString, databaseNamePrefix);
            ConnectionString = derivedConnectionString;
            Name = databaseName;
        }

        /// <summary>
        /// Returns the connection string of the database that was created
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Returns the name of the database that was created
        /// </summary>
        public string Name { get; }

        private void DropDatabaseIfCreated()
        {
            if (!databaseCreated)
                return;

            using (var connection = new NpgsqlConnection(this.originalConnectionString))
            {
                connection.Open();

                // Revoke future connections
                using (var cmd = new NpgsqlCommand($"REVOKE CONNECT ON DATABASE {Name} FROM public", connection))
                {
                    cmd.ExecuteNonQuery();
                }

                // Terminate all connections
                using (var cmd = new NpgsqlCommand($"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{Name}'", connection))
                {
                    cmd.ExecuteNonQuery();
                }

                // Drop database
                using (var cmd = new NpgsqlCommand($"DROP DATABASE {Name}", connection))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static (string connectionString, string databaseName) DeriveThrowawayConnectionString(string originalConnectionString, string databaseNamePrefix)
        {
            var builder = new NpgsqlConnectionStringBuilder(originalConnectionString);
            var databasePrefix = string.IsNullOrWhiteSpace(databaseNamePrefix)
                ? DefaultDatabaseNamePrefix
                : databaseNamePrefix;

            var databaseName = $"{databasePrefix}{Guid.NewGuid().ToString("n").Substring(0, 10).ToLowerInvariant()}";

            if (builder.TryGetValue("Database", out var initialDb))
            {
                builder.Remove("Database");
            }

            builder.Database = databaseName;
            return (builder.ConnectionString, databaseName);
        }

        public static ThrowawayDatabase Create(string username, string password, string host, int port, string databaseNamePrefix = null)
        {
            var connectionString = $"Host={host}; Port={port}; Username={username}; Password={password}; Database=postgres";
            return Create(connectionString, databaseNamePrefix);
        }

        public static ThrowawayDatabase Create(string username, string password, string host, string databaseNamePrefix = null)
        {
            var connectionString = $"Host={host}; Port={DefaultDatabasePort}; Username={username}; Password={password}; Database=postgres";
            return Create(connectionString, databaseNamePrefix);
        }

        public static ThrowawayDatabase Create(string connectionString, string databaseNamePrefix = null)
        {
            if (!TryPingDatabase(connectionString))
            {
                throw new Exception("Could not connect to the database");
            }

            var database = new ThrowawayDatabase(connectionString, databaseNamePrefix);
            if (!database.CreateDatabaseIfDoesNotExist())
            {
                throw new Exception("Could not create the throwaway database");
            }

            return database;
        }

        private bool CreateDatabaseIfDoesNotExist()
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(this.ConnectionString);
                if (!builder.TryGetValue("Database", out var database))
                    return false;

                var databaseName = database.ToString();
                var connectionStringOfMaster = this.ConnectionString.Replace(databaseName, "postgres");
                using (var otherConnection = new NpgsqlConnection(connectionStringOfMaster))
                {
                    otherConnection.Open();
                    using (var createCmd = new NpgsqlCommand($"CREATE DATABASE {databaseName}", otherConnection))
                    {
                        createCmd.ExecuteNonQuery();
                        Debug.Print($"Successfully created database {databaseName}");
                        this.databaseCreated = true;
                    }
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        private static bool TryPingDatabase(string originalConnectionString)
        {
            try
            {
                using (var connection = new NpgsqlConnection(originalConnectionString))
                {
                    connection.Open();
                    using (var cmd = new NpgsqlCommand("select 1", connection))
                    {
                        var _ = cmd.ExecuteScalar();
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error while pinging the PostgreSQL server at '{originalConnectionString}'");
                Console.WriteLine(ex.Message);
                Console.ForegroundColor = ConsoleColor.White;
                return false;
            }
        }

        public void Dispose() => DropDatabaseIfCreated();
    }
}
