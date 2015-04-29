using System;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.Remoting;
using System.Transactions;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DapperTests_UBA
{
	[TestClass]
	public class QueryAbstractTypes
	{
		protected TransactionScope TransactionScope { get; set; }
		protected SqlConnection Connection { get; set; }

		[TestInitialize]
		public void InitializeTest()
		{
			try
			{
				TransactionScope = new TransactionScope();

				Connection = new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True");
				Connection.Open();

				Connection.Execute(@"
					CREATE TABLE Owner (
						OwnerId INT NOT NULL PRIMARY KEY,
						Name VARCHAR(100) NOT NULL
					);

					CREATE TABLE Animal (
						AnimalId INT NOT NULL PRIMARY KEY,
						Discriminator VARCHAR(100) NOT NULL,
						FeatherColour VARCHAR(100),
						FurColour VARCHAR(100),
						OwnerId INT REFERENCES Owner(OwnerId)
					);");
			}
			catch (Exception)
			{
				// HACK: [TestCleanup] isn't run if an exception occurs inside [TestInitialize]
				// http://stackoverflow.com/questions/17788466/when-mstest-fails-in-testinitialize-why-doesnt-testcleanup-get-executed
				CleanUpTest();
				throw;
			}
		}

		[TestCleanup]
		public void CleanUpTest()
		{
			TransactionScope.Dispose();
			Connection.Close();
		}

		[TestMethod]
		public void QueryTest()
		{
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FeatherColour) VALUES (1, 'Bird', 'Red')");
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FurColour) VALUES (2, 'Dog', 'Black')");

			var animals = Connection.Query<Animal>("SELECT * FROM Animal").ToList();

			Assert.AreEqual(2, animals.Count);
		}

		[TestMethod]
		public void QueryMultiMapTest()
		{
			Connection.Execute("INSERT INTO Owner(OwnerId, Name) VALUES (1, 'Andrew')");

			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FeatherColour) VALUES (1, 'Bird', 'Red')");
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FurColour, OwnerId) VALUES (2, 'Dog', 'Black', 1)");

			var animals = Connection.Query<Animal, Owner, Animal>(@"
				SELECT Animal.*, '_' as split, Owner.* 
				FROM Animal
				LEFT JOIN Owner ON Animal.OwnerId = Owner.OwnerId", MapAnimal, splitOn: "split").ToList();

			Assert.AreEqual(2, animals.Count);

			var dog = animals.Single(a => a.AnimalId == 2);
			Assert.IsNotNull(dog.Owner);
			Assert.AreEqual(1, dog.Owner.OwnerId);
		}

		private Animal MapAnimal(Animal animal, Owner owner)
		{
			animal.Owner = owner;
			return animal;
		}

		[TestMethod]
		public void QueryMultipleTest()
		{
			Connection.Execute("INSERT INTO Owner(OwnerId, Name) VALUES (1, 'Andrew')");

			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FeatherColour) VALUES (1, 'Bird', 'Red')");
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FurColour, OwnerId) VALUES (2, 'Dog', 'Black', 1)");

			const string multiSql = @"
				SELECT Animal.*, '_' as split, Owner.* FROM Animal LEFT JOIN Owner ON Animal.OwnerId = Owner.OwnerId;
				SELECT Animal.*, '_' as split, Owner.* FROM Animal LEFT JOIN Owner ON Animal.OwnerId = Owner.OwnerId";

			using (var multi = Connection.QueryMultiple(multiSql, true))
			{
				var animals = multi.Read<Animal, Owner, Animal>(MapAnimal, splitOn: "split").ToList();
				var animals2 = multi.Read<Animal, Owner, Animal>(MapAnimal, splitOn: "split").ToList();

				Assert.AreEqual(2, animals.Count);
				Assert.AreEqual(2, animals2.Count);
			}
		}
	}
}
