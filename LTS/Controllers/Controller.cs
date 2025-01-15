using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using LTS.Services;

namespace LTS.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class Controller : ControllerBase
    {
        private readonly IConsume IConsumeInstance;

        public Controller(IConsume _IConsume)
        {
            IConsumeInstance = _IConsume;
        }

        [HttpGet("StartConsuming")]
        public void Start()
        {
            IConsumeInstance.Start();
            IConsumeInstance.StartConsume();   
        }

        [HttpGet("StopConsuming")]
        public void Stop()
        {
            IConsumeInstance.StopConsume();
        }

        //[HttpPost("AddParameter")]
        //public IActionResult Add([FromBody] string uavName , string parameter)
        //{
        //    IConsumeInstance.AddParameter(uavName , parameter);
        //    return Ok(new { message = "Parameter added successfully" });
        //}

        //[HttpPost("RemoveParameter")]
        //public IActionResult Remove([FromBody] string uavName, string parameter  )
        //{
        //    IConsumeInstance.RemoveParameter(parameter);
        //    return Ok(new { message = "Parameter removed successfully" });

        //}
    }
}
