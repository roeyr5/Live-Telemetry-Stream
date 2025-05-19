using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using LTS.Services;
using LTS.Entities;

namespace LTS.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class Controller : ControllerBase
    {
        private readonly IConsume _iconsumeInstance;

        public Controller(IConsume _IConsume)
        {
            _iconsumeInstance = _IConsume;
        }

        [HttpPost("AddTopic")]
        public OperationResult AddNewTopic([FromBody] ChannelDTO request)
        {
            return _iconsumeInstance.Subscribe(request.uavNumber); 
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
