using MovieLensOLAP_MVC.Controllers;

namespace MovieLensOLAP_MVC
{
    class Program
    {
        static void Main(string[] args)
        {
            var controller = new Controller();
            controller.Run();
        }
    }
}
