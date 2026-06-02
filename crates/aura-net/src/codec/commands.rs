//! PVAccess protocol command constants.
//!
//! Every PVA message has a `command` byte in its header that identifies
//! the message type. This module defines all known command codes from
//! the PVAccess Protocol Specification (v2).
//!
//! Two categories:
//! - **Application messages** (flags bit 0 = 0): carry data payloads.
//! - **Control messages** (flags bit 0 = 1): flow control, no payload.

pub const CMD_BEACON: u8 = 0x00;
pub const CMD_CONNECTION_VALIDATION: u8 = 0x01;
pub const CMD_ECHO: u8 = 0x02;
pub const CMD_SEARCH: u8 = 0x03;
pub const CMD_SEARCH_RESPONSE: u8 = 0x04;
pub const CMD_AUTHNZ: u8 = 0x05;
pub const CMD_ACL_CHANGE: u8 = 0x06;
pub const CMD_CREATE_CHANNEL: u8 = 0x07;
pub const CMD_DESTROY_CHANNEL: u8 = 0x08;
pub const CMD_CONNECTION_VALIDATED: u8 = 0x09;
pub const CMD_GET: u8 = 0x0A;
pub const CMD_PUT: u8 = 0x0B;
pub const CMD_PUT_GET: u8 = 0x0C;
pub const CMD_MONITOR: u8 = 0x0D;
pub const CMD_ARRAY: u8 = 0x0E;
pub const CMD_DESTROY_REQUEST: u8 = 0x0F;
pub const CMD_PROCESS: u8 = 0x10;
pub const CMD_GET_FIELD: u8 = 0x11;
pub const CMD_MESSAGE: u8 = 0x12;
pub const CMD_MULTIPLE_DATA: u8 = 0x13;
pub const CMD_RPC: u8 = 0x14;
pub const CMD_CANCEL_REQUEST: u8 = 0x15;
pub const CMD_ORIGIN_TAG: u8 = 0x16;

/// Total number of application commands.
pub const APP_COMMAND_COUNT: usize = CMD_ORIGIN_TAG as usize + 1;

/// All application command codes in order.
pub const ALL_APP_COMMANDS: [u8; APP_COMMAND_COUNT] = [
    CMD_BEACON,
    CMD_CONNECTION_VALIDATION,
    CMD_ECHO,
    CMD_SEARCH,
    CMD_SEARCH_RESPONSE,
    CMD_AUTHNZ,
    CMD_ACL_CHANGE,
    CMD_CREATE_CHANNEL,
    CMD_DESTROY_CHANNEL,
    CMD_CONNECTION_VALIDATED,
    CMD_GET,
    CMD_PUT,
    CMD_PUT_GET,
    CMD_MONITOR,
    CMD_ARRAY,
    CMD_DESTROY_REQUEST,
    CMD_PROCESS,
    CMD_GET_FIELD,
    CMD_MESSAGE,
    CMD_MULTIPLE_DATA,
    CMD_RPC,
    CMD_CANCEL_REQUEST,
    CMD_ORIGIN_TAG,
];

pub const CTRL_MARK_TOTAL_SENT: u8 = 0x00;
pub const CTRL_ACK_RECEIVED: u8 = 0x01;
pub const CTRL_SET_BYTE_ORDER: u8 = 0x02;
pub const CTRL_ECHO_REQUEST: u8 = 0x03;
pub const CTRL_ECHO_RESPONSE: u8 = 0x04;

/// Total number of control commands.
pub const CTRL_COMMAND_COUNT: usize = CTRL_ECHO_RESPONSE as usize + 1;

/// All control command codes in order.
pub const ALL_CTRL_COMMANDS: [u8; CTRL_COMMAND_COUNT] = [
    CTRL_MARK_TOTAL_SENT,
    CTRL_ACK_RECEIVED,
    CTRL_SET_BYTE_ORDER,
    CTRL_ECHO_REQUEST,
    CTRL_ECHO_RESPONSE,
];

pub const MONITOR_INIT: u8 = 0x08;
pub const MONITOR_START: u8 = 0x04;
pub const MONITOR_STOP: u8 = 0x02;
pub const MONITOR_DESTROY: u8 = 0x10;
pub const MONITOR_PIPELINE: u8 = 0x80;

/// All monitor sub-command codes.
pub const ALL_MONITOR_SUBS: [u8; 5] = [
    MONITOR_INIT,
    MONITOR_START,
    MONITOR_STOP,
    MONITOR_DESTROY,
    MONITOR_PIPELINE,
];

/// Human-readable name for an application command.
pub const fn app_command_name(cmd: u8) -> &'static str {
    match cmd {
        CMD_BEACON => "BEACON",
        CMD_CONNECTION_VALIDATION => "CONNECTION_VALIDATION",
        CMD_ECHO => "ECHO",
        CMD_SEARCH => "SEARCH",
        CMD_SEARCH_RESPONSE => "SEARCH_RESPONSE",
        CMD_AUTHNZ => "AUTHNZ",
        CMD_ACL_CHANGE => "ACL_CHANGE",
        CMD_CREATE_CHANNEL => "CREATE_CHANNEL",
        CMD_DESTROY_CHANNEL => "DESTROY_CHANNEL",
        CMD_CONNECTION_VALIDATED => "CONNECTION_VALIDATED",
        CMD_GET => "GET",
        CMD_PUT => "PUT",
        CMD_PUT_GET => "PUT_GET",
        CMD_MONITOR => "MONITOR",
        CMD_ARRAY => "ARRAY",
        CMD_DESTROY_REQUEST => "DESTROY_REQUEST",
        CMD_PROCESS => "PROCESS",
        CMD_GET_FIELD => "GET_FIELD",
        CMD_MESSAGE => "MESSAGE",
        CMD_MULTIPLE_DATA => "MULTIPLE_DATA",
        CMD_RPC => "RPC",
        CMD_CANCEL_REQUEST => "CANCEL_REQUEST",
        CMD_ORIGIN_TAG => "ORIGIN_TAG",
        _ => "UNKNOWN",
    }
}

/// Human-readable name for a control command.
pub const fn ctrl_command_name(cmd: u8) -> &'static str {
    match cmd {
        CTRL_MARK_TOTAL_SENT => "MARK_TOTAL_SENT",
        CTRL_ACK_RECEIVED => "ACK_RECEIVED",
        CTRL_SET_BYTE_ORDER => "SET_BYTE_ORDER",
        CTRL_ECHO_REQUEST => "ECHO_REQUEST",
        CTRL_ECHO_RESPONSE => "ECHO_RESPONSE",
        _ => "UNKNOWN_CTRL",
    }
}

/// Whether a command code is a known application command.
pub const fn is_known_app_command(cmd: u8) -> bool {
    cmd <= CMD_ORIGIN_TAG
}

/// Whether a command code is a known control command.
pub const fn is_known_ctrl_command(cmd: u8) -> bool {
    cmd <= CTRL_ECHO_RESPONSE
}

/// Monitor sub-command name.
pub const fn monitor_sub_name(sub: u8) -> &'static str {
    match sub {
        MONITOR_INIT => "INIT",
        MONITOR_START => "START",
        MONITOR_STOP => "STOP",
        MONITOR_DESTROY => "DESTROY",
        MONITOR_PIPELINE => "PIPELINE",
        _ => "UNKNOWN_SUB",
    }
}

/// Whether a byte is a known monitor sub-command.
pub const fn is_known_monitor_sub(sub: u8) -> bool {
    matches!(
        sub,
        MONITOR_INIT | MONITOR_START | MONITOR_STOP | MONITOR_DESTROY | MONITOR_PIPELINE
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmd_beacon() {
        assert_eq!(CMD_BEACON, 0x00);
    }
    #[test]
    fn test_cmd_connection_validation() {
        assert_eq!(CMD_CONNECTION_VALIDATION, 0x01);
    }
    #[test]
    fn test_cmd_echo() {
        assert_eq!(CMD_ECHO, 0x02);
    }
    #[test]
    fn test_cmd_search() {
        assert_eq!(CMD_SEARCH, 0x03);
    }
    #[test]
    fn test_cmd_search_response() {
        assert_eq!(CMD_SEARCH_RESPONSE, 0x04);
    }
    #[test]
    fn test_cmd_authnz() {
        assert_eq!(CMD_AUTHNZ, 0x05);
    }
    #[test]
    fn test_cmd_acl_change() {
        assert_eq!(CMD_ACL_CHANGE, 0x06);
    }
    #[test]
    fn test_cmd_create_channel() {
        assert_eq!(CMD_CREATE_CHANNEL, 0x07);
    }
    #[test]
    fn test_cmd_destroy_channel() {
        assert_eq!(CMD_DESTROY_CHANNEL, 0x08);
    }
    #[test]
    fn test_cmd_connection_validated() {
        assert_eq!(CMD_CONNECTION_VALIDATED, 0x09);
    }
    #[test]
    fn test_cmd_get() {
        assert_eq!(CMD_GET, 0x0A);
    }
    #[test]
    fn test_cmd_put() {
        assert_eq!(CMD_PUT, 0x0B);
    }
    #[test]
    fn test_cmd_put_get() {
        assert_eq!(CMD_PUT_GET, 0x0C);
    }
    #[test]
    fn test_cmd_monitor() {
        assert_eq!(CMD_MONITOR, 0x0D);
    }
    #[test]
    fn test_cmd_array() {
        assert_eq!(CMD_ARRAY, 0x0E);
    }
    #[test]
    fn test_cmd_destroy_request() {
        assert_eq!(CMD_DESTROY_REQUEST, 0x0F);
    }
    #[test]
    fn test_cmd_process() {
        assert_eq!(CMD_PROCESS, 0x10);
    }
    #[test]
    fn test_cmd_get_field() {
        assert_eq!(CMD_GET_FIELD, 0x11);
    }
    #[test]
    fn test_cmd_message() {
        assert_eq!(CMD_MESSAGE, 0x12);
    }
    #[test]
    fn test_cmd_multiple_data() {
        assert_eq!(CMD_MULTIPLE_DATA, 0x13);
    }
    #[test]
    fn test_cmd_rpc() {
        assert_eq!(CMD_RPC, 0x14);
    }
    #[test]
    fn test_cmd_cancel_request() {
        assert_eq!(CMD_CANCEL_REQUEST, 0x15);
    }
    #[test]
    fn test_cmd_origin_tag() {
        assert_eq!(CMD_ORIGIN_TAG, 0x16);
    }

    #[test]
    fn test_ctrl_mark_total_sent() {
        assert_eq!(CTRL_MARK_TOTAL_SENT, 0x00);
    }
    #[test]
    fn test_ctrl_ack_received() {
        assert_eq!(CTRL_ACK_RECEIVED, 0x01);
    }
    #[test]
    fn test_ctrl_set_byte_order() {
        assert_eq!(CTRL_SET_BYTE_ORDER, 0x02);
    }
    #[test]
    fn test_ctrl_echo_request() {
        assert_eq!(CTRL_ECHO_REQUEST, 0x03);
    }
    #[test]
    fn test_ctrl_echo_response() {
        assert_eq!(CTRL_ECHO_RESPONSE, 0x04);
    }

    #[test]
    fn test_monitor_init() {
        assert_eq!(MONITOR_INIT, 0x08);
    }
    #[test]
    fn test_monitor_start() {
        assert_eq!(MONITOR_START, 0x04);
    }
    #[test]
    fn test_monitor_stop() {
        assert_eq!(MONITOR_STOP, 0x02);
    }
    #[test]
    fn test_monitor_destroy() {
        assert_eq!(MONITOR_DESTROY, 0x10);
    }
    #[test]
    fn test_monitor_pipeline() {
        assert_eq!(MONITOR_PIPELINE, 0x80);
    }

    #[test]
    fn test_all_app_count() {
        assert_eq!(ALL_APP_COMMANDS.len(), 23);
        assert_eq!(APP_COMMAND_COUNT, 23);
    }
    #[test]
    fn test_all_app_contiguous() {
        for (i, &cmd) in ALL_APP_COMMANDS.iter().enumerate() {
            assert_eq!(cmd, i as u8);
        }
    }
    #[test]
    fn test_all_ctrl_count() {
        assert_eq!(ALL_CTRL_COMMANDS.len(), 5);
        assert_eq!(CTRL_COMMAND_COUNT, 5);
    }
    #[test]
    fn test_all_ctrl_contiguous() {
        for (i, &cmd) in ALL_CTRL_COMMANDS.iter().enumerate() {
            assert_eq!(cmd, i as u8);
        }
    }
    #[test]
    fn test_all_monitor_count() {
        assert_eq!(ALL_MONITOR_SUBS.len(), 5);
    }
    #[test]
    fn test_all_monitor_unique() {
        for (i, &a) in ALL_MONITOR_SUBS.iter().enumerate() {
            for (j, &b) in ALL_MONITOR_SUBS.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b, "monitor subs {i} and {j} collide");
                }
            }
        }
    }

    #[test]
    fn test_name_beacon() {
        assert_eq!(app_command_name(CMD_BEACON), "BEACON");
    }
    #[test]
    fn test_name_conn_validation() {
        assert_eq!(
            app_command_name(CMD_CONNECTION_VALIDATION),
            "CONNECTION_VALIDATION"
        );
    }
    #[test]
    fn test_name_echo() {
        assert_eq!(app_command_name(CMD_ECHO), "ECHO");
    }
    #[test]
    fn test_name_search() {
        assert_eq!(app_command_name(CMD_SEARCH), "SEARCH");
    }
    #[test]
    fn test_name_search_response() {
        assert_eq!(app_command_name(CMD_SEARCH_RESPONSE), "SEARCH_RESPONSE");
    }
    #[test]
    fn test_name_authnz() {
        assert_eq!(app_command_name(CMD_AUTHNZ), "AUTHNZ");
    }
    #[test]
    fn test_name_acl_change() {
        assert_eq!(app_command_name(CMD_ACL_CHANGE), "ACL_CHANGE");
    }
    #[test]
    fn test_name_create_channel() {
        assert_eq!(app_command_name(CMD_CREATE_CHANNEL), "CREATE_CHANNEL");
    }
    #[test]
    fn test_name_destroy_channel() {
        assert_eq!(app_command_name(CMD_DESTROY_CHANNEL), "DESTROY_CHANNEL");
    }
    #[test]
    fn test_name_conn_validated() {
        assert_eq!(
            app_command_name(CMD_CONNECTION_VALIDATED),
            "CONNECTION_VALIDATED"
        );
    }
    #[test]
    fn test_name_get() {
        assert_eq!(app_command_name(CMD_GET), "GET");
    }
    #[test]
    fn test_name_put() {
        assert_eq!(app_command_name(CMD_PUT), "PUT");
    }
    #[test]
    fn test_name_put_get() {
        assert_eq!(app_command_name(CMD_PUT_GET), "PUT_GET");
    }
    #[test]
    fn test_name_monitor() {
        assert_eq!(app_command_name(CMD_MONITOR), "MONITOR");
    }
    #[test]
    fn test_name_array() {
        assert_eq!(app_command_name(CMD_ARRAY), "ARRAY");
    }
    #[test]
    fn test_name_destroy_request() {
        assert_eq!(app_command_name(CMD_DESTROY_REQUEST), "DESTROY_REQUEST");
    }
    #[test]
    fn test_name_process() {
        assert_eq!(app_command_name(CMD_PROCESS), "PROCESS");
    }
    #[test]
    fn test_name_get_field() {
        assert_eq!(app_command_name(CMD_GET_FIELD), "GET_FIELD");
    }
    #[test]
    fn test_name_message() {
        assert_eq!(app_command_name(CMD_MESSAGE), "MESSAGE");
    }
    #[test]
    fn test_name_multiple_data() {
        assert_eq!(app_command_name(CMD_MULTIPLE_DATA), "MULTIPLE_DATA");
    }
    #[test]
    fn test_name_rpc() {
        assert_eq!(app_command_name(CMD_RPC), "RPC");
    }
    #[test]
    fn test_name_cancel_request() {
        assert_eq!(app_command_name(CMD_CANCEL_REQUEST), "CANCEL_REQUEST");
    }
    #[test]
    fn test_name_origin_tag() {
        assert_eq!(app_command_name(CMD_ORIGIN_TAG), "ORIGIN_TAG");
    }
    #[test]
    fn test_name_unknown() {
        assert_eq!(app_command_name(0xFF), "UNKNOWN");
    }
    #[test]
    fn test_name_one_past_last() {
        assert_eq!(app_command_name(0x17), "UNKNOWN");
    }

    #[test]
    fn test_ctrl_name_mark() {
        assert_eq!(ctrl_command_name(CTRL_MARK_TOTAL_SENT), "MARK_TOTAL_SENT");
    }
    #[test]
    fn test_ctrl_name_ack() {
        assert_eq!(ctrl_command_name(CTRL_ACK_RECEIVED), "ACK_RECEIVED");
    }
    #[test]
    fn test_ctrl_name_order() {
        assert_eq!(ctrl_command_name(CTRL_SET_BYTE_ORDER), "SET_BYTE_ORDER");
    }
    #[test]
    fn test_ctrl_name_echo_req() {
        assert_eq!(ctrl_command_name(CTRL_ECHO_REQUEST), "ECHO_REQUEST");
    }
    #[test]
    fn test_ctrl_name_echo_resp() {
        assert_eq!(ctrl_command_name(CTRL_ECHO_RESPONSE), "ECHO_RESPONSE");
    }
    #[test]
    fn test_ctrl_name_unknown() {
        assert_eq!(ctrl_command_name(0x05), "UNKNOWN_CTRL");
    }
    #[test]
    fn test_ctrl_name_0xff() {
        assert_eq!(ctrl_command_name(0xFF), "UNKNOWN_CTRL");
    }

    #[test]
    fn test_sub_name_init() {
        assert_eq!(monitor_sub_name(MONITOR_INIT), "INIT");
    }
    #[test]
    fn test_sub_name_start() {
        assert_eq!(monitor_sub_name(MONITOR_START), "START");
    }
    #[test]
    fn test_sub_name_stop() {
        assert_eq!(monitor_sub_name(MONITOR_STOP), "STOP");
    }
    #[test]
    fn test_sub_name_destroy() {
        assert_eq!(monitor_sub_name(MONITOR_DESTROY), "DESTROY");
    }
    #[test]
    fn test_sub_name_pipeline() {
        assert_eq!(monitor_sub_name(MONITOR_PIPELINE), "PIPELINE");
    }
    #[test]
    fn test_sub_name_unknown() {
        assert_eq!(monitor_sub_name(0x00), "UNKNOWN_SUB");
    }
    #[test]
    fn test_sub_name_0xff() {
        assert_eq!(monitor_sub_name(0xFF), "UNKNOWN_SUB");
    }
    #[test]
    fn test_sub_name_0x01() {
        assert_eq!(monitor_sub_name(0x01), "UNKNOWN_SUB");
    }

    #[test]
    fn test_known_app_first() {
        assert!(is_known_app_command(CMD_BEACON));
    }
    #[test]
    fn test_known_app_last() {
        assert!(is_known_app_command(CMD_ORIGIN_TAG));
    }
    #[test]
    fn test_known_app_mid() {
        assert!(is_known_app_command(CMD_MONITOR));
    }
    #[test]
    fn test_unknown_app_17() {
        assert!(!is_known_app_command(0x17));
    }
    #[test]
    fn test_unknown_app_ff() {
        assert!(!is_known_app_command(0xFF));
    }

    #[test]
    fn test_known_ctrl_first() {
        assert!(is_known_ctrl_command(CTRL_MARK_TOTAL_SENT));
    }
    #[test]
    fn test_known_ctrl_last() {
        assert!(is_known_ctrl_command(CTRL_ECHO_RESPONSE));
    }
    #[test]
    fn test_unknown_ctrl_05() {
        assert!(!is_known_ctrl_command(0x05));
    }
    #[test]
    fn test_unknown_ctrl_ff() {
        assert!(!is_known_ctrl_command(0xFF));
    }

    #[test]
    fn test_known_sub_init() {
        assert!(is_known_monitor_sub(MONITOR_INIT));
    }
    #[test]
    fn test_known_sub_start() {
        assert!(is_known_monitor_sub(MONITOR_START));
    }
    #[test]
    fn test_known_sub_stop() {
        assert!(is_known_monitor_sub(MONITOR_STOP));
    }
    #[test]
    fn test_known_sub_destroy() {
        assert!(is_known_monitor_sub(MONITOR_DESTROY));
    }
    #[test]
    fn test_known_sub_pipeline() {
        assert!(is_known_monitor_sub(MONITOR_PIPELINE));
    }
    #[test]
    fn test_unknown_sub_0x00() {
        assert!(!is_known_monitor_sub(0x00));
    }
    #[test]
    fn test_unknown_sub_0x01() {
        assert!(!is_known_monitor_sub(0x01));
    }
    #[test]
    fn test_unknown_sub_0xff() {
        assert!(!is_known_monitor_sub(0xFF));
    }

    #[test]
    fn test_all_app_named() {
        for &cmd in &ALL_APP_COMMANDS {
            assert_ne!(app_command_name(cmd), "UNKNOWN", "0x{cmd:02X}");
        }
    }
    #[test]
    fn test_all_ctrl_named() {
        for &cmd in &ALL_CTRL_COMMANDS {
            assert_ne!(ctrl_command_name(cmd), "UNKNOWN_CTRL", "0x{cmd:02X}");
        }
    }
    #[test]
    fn test_all_monitor_named() {
        for &sub in &ALL_MONITOR_SUBS {
            assert_ne!(monitor_sub_name(sub), "UNKNOWN_SUB", "0x{sub:02X}");
        }
    }

    #[test]
    fn test_monitor_subs_are_powers_or_bit_patterns() {
        // Each sub-command should have exactly one bit set.
        for &sub in &ALL_MONITOR_SUBS {
            assert_eq!(sub.count_ones(), 1, "0x{sub:02X} should be a single bit");
        }
    }
}
