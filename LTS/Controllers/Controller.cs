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
        private readonly IConsume IConsumeInstance;

        public Controller(IConsume _IConsume)
        {
            IConsumeInstance = _IConsume;
        }

        [HttpPost("AddTopic")]
        public Task<OperationResult> AddNewTopic([FromBody] ChannelDTO request)
        {
            return IConsumeInstance.Subscribe(request.uavNumber); 
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
