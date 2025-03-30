using Microsoft.EntityFrameworkCore;

namespace Consumer;

public class TestDbContext : DbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options)
        : base(options)
    {
        
    }
    
    public DbSet<Order> Orders { get; set; }
}