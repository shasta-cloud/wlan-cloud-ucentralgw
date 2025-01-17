//
// Created by stephane bourque on 2021-07-21.
//

#include "RESTAPI_deviceDashboardHandler.h"
#include "Daemon.h"
#include "Dashboard.h"

namespace OpenWifi {
	void RESTAPI_deviceDashboardHandler::DoGet() {
		poco_information(Logger(),fmt::format("GET-DASHBOARD: {}", Requester()));
		GWObjects::Dashboard	Data;
		if(Daemon()->GetDashboard().Get(Data, Logger())) {
			Poco::JSON::Object Answer;
			Data.to_json(Answer);
			return ReturnObject(Answer);
		}
		return BadRequest(RESTAPI::Errors::InternalError);
	}
}