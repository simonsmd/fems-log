use std::time::Duration;

use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver},
    time::timeout,
};

use futures::{SinkExt, TryStreamExt};
use jsonrpc_lite::JsonRpc;
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, Message, Utf8Bytes},
};
use tracing::error;
use uuid::Uuid;
use diesel::Insertable;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Websocket communication error")]
    Websocket(#[from] tungstenite::Error),
    #[error("Failed to (de)serialize JsonRPC message")]
    Serialization(#[from] serde_json::Error),
    #[error("Request timed out")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Websocket closed")]
    WebsocketClosed,
    #[error("Invalid response message type")]
    InvalidMessageType(Message),
    #[error("No result returned")]
    NoResult(JsonRpc),
}

pub struct Fems {
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Fems {
    pub async fn connect(url: &str, password: &str) -> Result<Self, Error> {
        let (websocket, _) = tokio_tungstenite::connect_async(url).await?;

        let mut fems = Self { websocket };

        let request = JsonRpc::request_with_params(
            Uuid::new_v4().to_string(),
            "authenticateWithPassword",
            json!({"password": password}),
        );
        fems.websocket
            .send(Message::Text(serde_json::to_string(&request)?.into()))
            .await?;
        fems.next_message().await?;

        Ok(fems)
    }

    async fn next_message(&mut self) -> Result<Utf8Bytes, Error> {
        let message = timeout(Duration::from_secs(10), self.websocket.try_next())
            .await??
            .ok_or(Error::WebsocketClosed)?;

        match message {
            Message::Text(utf8_bytes) => Ok(utf8_bytes),
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                Err(Error::InvalidMessageType(message))
            }
            Message::Close(_) => Err(Error::WebsocketClosed),
        }
    }

    fn edge_rpc(edge_id: &str, payload: JsonRpc) -> Result<JsonRpc, Error> {
        let payload = serde_json::to_value(payload)?;
        Ok(JsonRpc::request_with_params(
            Uuid::new_v4().to_string(),
            "edgeRpc",
            json!({"edgeId": edge_id, "payload": payload}),
        ))
    }

    fn component_json_api(component_id: &str, payload: JsonRpc) -> Result<JsonRpc, Error> {
        let payload = serde_json::to_value(payload)?;
        Ok(JsonRpc::request_with_params(
            Uuid::new_v4().to_string(),
            "componentJsonApi",
            json!({"componentId": component_id, "payload": payload}),
        ))
    }

    pub async fn get_channels_of_component(&mut self, component_id: &str) -> Result<GetChannelsResponse, Error> {
        let request = JsonRpc::request_with_params(
            Uuid::new_v4().to_string(),
            "getChannelsOfComponent",
            json!({"componentId": component_id}),
        );
        let request = Fems::edge_rpc("0", Fems::component_json_api("_componentManager", request)?)?;

        self.websocket
            .send(tungstenite::Message::Text(
                serde_json::to_string(&request)?.into(),
            ))
            .await?;
        let edge_rpc_response: JsonRpc = serde_json::from_str(self.next_message().await?.as_str())?;
        println!("{:?}", edge_rpc_response.get_result());
        let response: JsonRpc = serde_json::from_value(
            edge_rpc_response
                .get_result()
                .ok_or_else(|| Error::NoResult(edge_rpc_response.clone()))?
                .get("payload")
                .unwrap()
                .clone(),
        )?;
        let result: GetChannelsResponse = serde_json::from_value(
            response
                .get_result()
                .ok_or_else(|| Error::NoResult(response.clone()))?
                .clone(),
        )?;

        Ok(result)
    }

    pub async fn subscribe_channels(mut self) -> Result<Receiver<ChannelData>, Error> {
        let request = JsonRpc::request_with_params(
            Uuid::new_v4().to_string(),
            "subscribeChannels",
            json!(
                {
                    "count": 0,
                    "channels": [
                        "_sum/GridToConsumptionEnergy",
                        "_sum/GridSellActiveEnergy",
                        "_sum/ProductionToEssPower",
                        "_sum/ConsumptionActiveEnergy",
                        "_sum/EssMaxApparentPower",
                        "_sum/ProductionDcActiveEnergy",
                        "_sum/ConsumptionActivePower",
                        "_sum/GridMaxActivePower",
                        "_sum/EssCapacity",
                        "_sum/EssToConsumptionEnergy",
                        "_sum/ConsumptionMaxActivePower",
                        "_sum/UnmanagedConsumptionActivePower",
                        "_sum/EssActiveChargeEnergy",
                        "_sum/GridActivePowerL2",
                        "_sum/GridActivePowerL1",
                        "_sum/EssMinDischargePower",
                        "_sum/ProductionDcActualPower",
                        "_sum/EssActiveDischargeEnergy",
                        "_sum/ProductionToConsumptionEnergy",
                        "_sum/GridToConsumptionPower",
                        "_sum/EssActivePower",
                        "_sum/EssToConsumptionPower",
                        "_sum/GridMinActivePower",
                        "_sum/ProductionActiveEnergy",
                        "_sum/EssSoc",
                        "_sum/ProductionToEssEnergy",
                        "_sum/ProductionToGridPower",
                        "_sum/ProductionToConsumptionPower",
                        "_sum/EssDcDischargeEnergy",
                        "_sum/GridBuyActiveEnergy",
                        "_sum/State",
                        "_sum/EssReactivePower",
                        "_sum/UnmanagedProductionActivePower",
                        "_sum/ProductionAcActivePowerL2",
                        "_sum/ProductionAcActivePowerL3",
                        "_sum/ProductionAcActivePower",
                        "_sum/ProductionAcActivePowerL1",
                        "_sum/ProductionActivePower",
                        "_sum/GridToEssEnergy",
                        "_sum/EssDischargePower",
                        "_sum/ConsumptionActivePowerL2",
                        "_sum/ConsumptionActivePowerL3",
                        "_sum/ConsumptionActivePowerL1",
                        "_sum/EssActivePowerL3",
                        "_sum/EssDcChargeEnergy",
                        "_sum/EssActivePowerL2",
                        "_sum/ProductionMaxActivePower",
                        "_sum/ProductionAcActiveEnergy",
                        "_sum/ProductionToGridEnergy",
                        "_sum/GridActivePowerL3",
                        "_sum/GridMode",
                        "_sum/EssActivePowerL1",
                        "_sum/GridToEssPower",
                        "_sum/EssMaxDischargePower",
                        "_sum/GridModeOffGridTime",
                        "_sum/GridActivePower",
                        "_sum/EssToGridEnergy",
                    ]
                }
            ),
        );
        let request = Fems::edge_rpc("0", request)?;

        self.websocket
            .send(tungstenite::Message::Text(
                serde_json::to_string(&request)?.into(),
            ))
            .await?;
        self.next_message().await?;

        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(async move {
            loop {
                match self.process_channel_update().await {
                    Ok(data) => {
                        if let Err(error) = tx.send(data).await {
                            error!("{error:#}");
                        }
                    }
                    Err(error) => error!("{error:#?}"),
                }
            }
        });

        Ok(rx)
    }

    async fn process_channel_update(&mut self) -> Result<ChannelData, Error> {
        let message: ChannelUpdateMessage =
            serde_json::from_str(self.next_message().await?.as_str())?;
        Ok(message.params.payload.params)
    }
}

#[derive(Debug, Deserialize)]
struct GetChannelsResponse {
    channels: Vec<Channel>,
}

#[derive(Debug, Deserialize)]
struct Channel {
    id: String,
    #[serde(rename = "type")]
    fems_type: String,
}

#[derive(Debug, Deserialize)]
struct ChannelUpdateMessage {
    params: ChannelUpdateParams,
}

#[derive(Debug, Deserialize)]
struct ChannelUpdateParams {
    payload: CurrentDataMessage,
}

#[derive(Debug, Deserialize)]
struct CurrentDataMessage {
    params: ChannelData,
}

#[derive(Debug, Deserialize, Insertable)]
#[diesel(table_name = crate::schema::ems)]
pub struct ChannelData {
    #[serde(rename = "_sum/GridToConsumptionEnergy")]
    grid_to_consumption_energy: Option<i64>,
    #[serde(rename = "_sum/GridSellActiveEnergy")]
    grid_sell_active_energy: Option<i64>,
    #[serde(rename = "_sum/ProductionToEssPower")]
    production_to_ess_power: Option<i32>,
    #[serde(rename = "_sum/ConsumptionActiveEnergy")]
    consumption_active_energy: Option<i64>,
    #[serde(rename = "_sum/EssMaxApparentPower")]
    ess_max_apparent_power: Option<i32>,
    #[serde(rename = "_sum/ProductionDcActiveEnergy")]
    production_dc_active_energy: Option<i64>,
    #[serde(rename = "_sum/ConsumptionActivePower")]
    consumption_active_power: Option<i32>,
    #[serde(rename = "_sum/GridMaxActivePower")]
    grid_max_active_power: Option<i32>,
    #[serde(rename = "_sum/EssCapacity")]
    ess_capacity: Option<i32>,
    #[serde(rename = "_sum/EssToConsumptionEnergy")]
    ess_to_consumption_energy: Option<i64>,
    #[serde(rename = "_sum/ConsumptionMaxActivePower")]
    consumption_max_active_power: Option<i32>,
    #[serde(rename = "_sum/UnmanagedConsumptionActivePower")]
    unmanaged_consumption_active_power: Option<i32>,
    #[serde(rename = "_sum/EssActiveChargeEnergy")]
    ess_active_charge_energy: Option<i64>,
    #[serde(rename = "_sum/GridActivePowerL2")]
    grid_active_power_l2: Option<i32>,
    #[serde(rename = "_sum/GridActivePowerL1")]
    grid_active_power_l1: Option<i32>,
    #[serde(rename = "_sum/EssMinDischargePower")]
    ess_min_discharge_power: Option<i32>,
    #[serde(rename = "_sum/ProductionDcActualPower")]
    production_dc_actual_power: Option<i32>,
    #[serde(rename = "_sum/EssActiveDischargeEnergy")]
    ess_active_discharge_energy: Option<i64>,
    #[serde(rename = "_sum/ProductionToConsumptionEnergy")]
    production_to_consumption_energy: Option<i64>,
    #[serde(rename = "_sum/GridToConsumptionPower")]
    grid_to_consumption_power: Option<i32>,
    #[serde(rename = "_sum/EssActivePower")]
    ess_active_power: Option<i32>,
    #[serde(rename = "_sum/EssToConsumptionPower")]
    ess_to_consumption_power: Option<i32>,
    #[serde(rename = "_sum/GridMinActivePower")]
    grid_min_active_power: Option<i32>,
    #[serde(rename = "_sum/ProductionActiveEnergy")]
    production_active_energy: Option<i64>,
    #[serde(rename = "_sum/EssSoc")]
    ess_soc: Option<i32>,
    #[serde(rename = "_sum/ProductionToEssEnergy")]
    production_to_ess_energy: Option<i64>,
    #[serde(rename = "_sum/ProductionToGridPower")]
    production_to_grid_power: Option<i32>,
    #[serde(rename = "_sum/ProductionToConsumptionPower")]
    production_to_consumption_power: Option<i32>,
    #[serde(rename = "_sum/EssDcDischargeEnergy")]
    ess_dc_discharge_energy: Option<i64>,
    #[serde(rename = "_sum/GridBuyActiveEnergy")]
    grid_buy_active_energy: Option<i64>,
    #[serde(rename = "_sum/State")]
    state: Option<i32>,
    #[serde(rename = "_sum/EssReactivePower")]
    ess_reactive_power: Option<i32>,
    #[serde(rename = "_sum/UnmanagedProductionActivePower")]
    unmanaged_production_active_power: Option<i32>,
    #[serde(rename = "_sum/ProductionAcActivePowerL2")]
    production_ac_active_power_l2: Option<i32>,
    #[serde(rename = "_sum/ProductionAcActivePowerL3")]
    production_ac_active_power_l3: Option<i32>,
    #[serde(rename = "_sum/ProductionAcActivePower")]
    production_ac_active_power: Option<i32>,
    #[serde(rename = "_sum/ProductionAcActivePowerL1")]
    production_ac_active_power_l1: Option<i32>,
    #[serde(rename = "_sum/ProductionActivePower")]
    production_active_power: Option<i32>,
    #[serde(rename = "_sum/GridToEssEnergy")]
    grid_to_ess_energy: Option<i64>,
    #[serde(rename = "_sum/EssDischargePower")]
    ess_discharge_power: Option<i32>,
    #[serde(rename = "_sum/ConsumptionActivePowerL2")]
    consumption_active_power_l2: Option<i32>,
    #[serde(rename = "_sum/ConsumptionActivePowerL3")]
    consumption_active_power_l3: Option<i32>,
    #[serde(rename = "_sum/ConsumptionActivePowerL1")]
    consumption_active_power_l1: Option<i32>,
    #[serde(rename = "_sum/EssActivePowerL3")]
    ess_active_power_l3: Option<i32>,
    #[serde(rename = "_sum/EssDcChargeEnergy")]
    ess_dc_charge_energy: Option<i64>,
    #[serde(rename = "_sum/EssActivePowerL2")]
    ess_active_power_l2: Option<i32>,
    #[serde(rename = "_sum/ProductionMaxActivePower")]
    production_max_active_power: Option<i32>,
    #[serde(rename = "_sum/ProductionAcActiveEnergy")]
    production_ac_active_energy: Option<i64>,
    #[serde(rename = "_sum/ProductionToGridEnergy")]
    production_to_grid_energy: Option<i64>,
    #[serde(rename = "_sum/GridActivePowerL3")]
    grid_active_power_l3: Option<i32>,
    #[serde(rename = "_sum/GridMode")]
    grid_mode: Option<i32>,
    #[serde(rename = "_sum/EssActivePowerL1")]
    ess_active_power_l1: Option<i32>,
    #[serde(rename = "_sum/GridToEssPower")]
    grid_to_ess_power: Option<i32>,
    #[serde(rename = "_sum/EssMaxDischargePower")]
    ess_max_discharge_power: Option<i32>,
    #[serde(rename = "_sum/GridModeOffGridTime")]
    grid_mode_off_grid_time: Option<i64>,
    #[serde(rename = "_sum/GridActivePower")]
    grid_active_power: Option<i32>,
    #[serde(rename = "_sum/EssToGridEnergy")]
    ess_to_grid_energy: Option<i64>,
}
