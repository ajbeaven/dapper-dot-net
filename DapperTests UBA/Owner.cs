using Dapper.Contrib.Extensions;

namespace DapperTests_UBA
{
	[Table("Owner")]
	public class Owner
	{
		[Key]
		public int OwnerId { get; set; }

		public string Name { get; set; }
	}
}