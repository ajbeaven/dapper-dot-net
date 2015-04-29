using Dapper.Contrib.Extensions;

namespace DapperTests_UBA
{
	[Table("Animal")]
	public abstract class Animal
	{
		[Key]
		public int AnimalId { get; set; }

		public int? OwnerId { get; set; }

		[Write(false)]
		public string Discriminator { get { return GetType().Name; } }

		[Write(false)]
		public Owner Owner { get; set; }
	}

	public class Bird : Animal
	{
		public string FeatherColour { get; set; }
	}

	public class Dog : Animal
	{
		public string FurColour { get; set; }
	}
}