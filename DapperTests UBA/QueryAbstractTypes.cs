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
					CREATE TABLE Animal (
						AnimalId INT NOT NULL PRIMARY KEY,
						Discriminator VARCHAR(100) NOT NULL,
						FeatherColour VARCHAR(100),
						FurColour VARCHAR(100),
						FriendId INT REFERENCES Animal(AnimalId)
					)");
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
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FeatherColour) VALUES (1, 'Bird', 'Red')");
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FurColour, FriendId) VALUES (2, 'Dog', 'Black', 1)");

			var animals = Connection.Query<Animal, Animal, Animal>(@"
				SELECT Animal.*, '_' as split, Friend.* 
				FROM Animal
				LEFT JOIN Animal AS Friend ON Animal.FriendId = Friend.AnimalId", MapAnimal, splitOn: "split").ToList();

			Assert.AreEqual(2, animals.Count);

			var dog = animals.Single(a => a.AnimalId == 2);
			Assert.IsNotNull(dog.Friend);
			Assert.AreEqual(1, dog.Friend.AnimalId);
		}

		private Animal MapAnimal(Animal animal, Animal friend)
		{
			animal.Friend = friend;
			return animal;
		}

		[TestMethod]
		public void QueryMultipleTest()
		{
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FeatherColour) VALUES (1, 'Bird', 'Red')");
			Connection.Execute("INSERT INTO Animal(AnimalId, Discriminator, FurColour, FriendId) VALUES (2, 'Dog', 'Black', 1)");

			const string multiSql = @"
				SELECT Animal.*, '_' as split, Friend.* FROM Animal LEFT JOIN Animal AS Friend ON Animal.FriendId = Friend.AnimalId;
				SELECT Animal.*, '_' as split, Friend.* FROM Animal LEFT JOIN Animal AS Friend ON Animal.FriendId = Friend.AnimalId";

			using (var multi = Connection.QueryMultiple(multiSql, true))
			{
				var animals = multi.Read<Animal, Animal, Animal>(MapAnimal, splitOn: "split").ToList();
				var animals2 = multi.Read<Animal, Animal, Animal>(MapAnimal, splitOn: "split").ToList();

				Assert.AreEqual(2, animals.Count);
				Assert.AreEqual(2, animals2.Count);
			}
		}
	}
}
