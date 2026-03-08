--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
local core           = require("apisix.core")
local config_local   = require("apisix.core.config_local")
local secret         = require("apisix.secret")
local plugin         = require("apisix.plugin")
local plugin_checker = require("apisix.plugin").plugin_checker
local check_schema   = require("apisix.core.schema").check
local error          = error
local ipairs         = ipairs
local pairs          = pairs
local next           = next
local type           = type
local string_sub     = string.sub
local string_find    = string.find
local consumers


local _M = {
    version = 0.3,
}

local lrucache = core.lrucache.new({
    ttl = 300, count = 512
})

-- Please calculate and set the value of the "consumers_count_for_lrucache"
-- variable based on the number of consumers in the current environment,
-- taking into account the appropriate adjustment coefficient.
local consumers_count_for_lrucache = 4096
local incremental_consumer_index_enabled = false
local plugin_indexes = {}
local credential_keys_by_consumer = {}
local credential_keys_full_sync_version = 0

local function remove_etcd_prefix(key)
    local prefix = ""
    local local_conf = config_local.local_conf()
    local role = core.table.try_read_attr(local_conf, "deployment", "role")
    local provider = core.table.try_read_attr(local_conf, "deployment", "role_" ..
    role, "config_provider")
    if provider == "etcd" and local_conf.etcd and local_conf.etcd.prefix then
        prefix = local_conf.etcd.prefix
    end
    return string_sub(key, #prefix + 1)
end

-- /{etcd.prefix}/consumers/{consumer_name}/credentials/{credential_id} --> {consumer_name}
local function get_consumer_name_from_credential_etcd_key(key)
    local uri_segs = core.utils.split_uri(remove_etcd_prefix(key))
    return uri_segs[3]
end

local function is_credential_etcd_key(key)
    if not key then
        return false
    end

    local uri_segs = core.utils.split_uri(remove_etcd_prefix(key))
    return uri_segs[2] == "consumers" and uri_segs[4] == "credentials"
end

local function get_credential_id_from_etcd_key(key)
    local uri_segs = core.utils.split_uri(remove_etcd_prefix(key))
    return uri_segs[5]
end

local function get_consumer_short_key(key)
    local short_key = remove_etcd_prefix(key)
    return string_sub(short_key, #"/consumers/" + 1)
end

local function is_consumer_short_key(key)
    return key and not string_find(key, "/credentials/", 1, true)
end

local function get_consumer_name_from_short_key(key)
    if not key then
        return nil
    end

    local pos = string_find(key, "/credentials/", 1, true)
    if not pos then
        return key
    end

    return string_sub(key, 1, pos - 1)
end

local function filter_consumers_list(data_list)
    if #data_list == 0 then
        return data_list
    end

    local list = {}
    for _, item in ipairs(data_list) do
        if not (type(item) == "table" and is_credential_etcd_key(item.key)) then
            core.table.insert(list, item)
        end
    end

    return list
end

local plugin_consumer
local construct_consumer_data
local get_filled_consumer
local create_consume_cache
do
    local consumers_id_lrucache = core.lrucache.new({
            count = consumers_count_for_lrucache
        })
    local consumer_lrucache = core.lrucache.new({
            count = consumers_count_for_lrucache
        })

function construct_consumer_data(val, name, plugin_config)
    -- if the val is a Consumer, clone it to the local consumer;
    -- if the val is a Credential, to get the Consumer by consumer_name and then clone
    -- it to the local consumer.
    local consumer
    if is_credential_etcd_key(val.key) then
        local consumer_name = get_consumer_name_from_credential_etcd_key(val.key)
        local the_consumer = consumers:get(consumer_name)
        if the_consumer and the_consumer.value then
            consumer = consumers_id_lrucache(val.value.id .. name, val.modifiedIndex ..
                                                the_consumer.modifiedIndex,
                function (val, the_consumer)
                    consumer = core.table.clone(the_consumer.value)
                    consumer.modifiedIndex = the_consumer.modifiedIndex
                    consumer.credential_id = get_credential_id_from_etcd_key(val.key)
                    return consumer
                end, val, the_consumer)
        else
            return nil, "failed to get the consumer for the credential, key: " .. val.key
        end
    else
        consumer = consumers_id_lrucache(val.value.id .. name, val.modifiedIndex,
            function (val)
                consumer = core.table.clone(val.value)
                consumer.modifiedIndex = val.modifiedIndex
                return consumer
            end, val)
    end

    if consumer.labels then
        consumer.custom_id = consumer.labels["custom_id"]
    end

    consumer.consumer_name = consumer.id
    consumer._etcd_key = get_consumer_short_key(val.key)
    consumer.auth_conf = plugin_config

    return consumer
end


local function fill_consumer_secret(consumer)
    local new_consumer = core.table.clone(consumer)
    new_consumer.auth_conf = secret.fetch_secrets(new_consumer.auth_conf, false)
    return new_consumer
end


function get_filled_consumer(consumer)
    return consumer_lrucache(consumer, nil, fill_consumer_secret, consumer)
end


function plugin_consumer()
    local plugins = {}

    if consumers.values == nil then
        return plugins
    end

    for _, val in ipairs(consumers.values) do
        if type(val) ~= "table" then
            goto CONTINUE
        end

        for name, config in pairs(val.value.plugins or {}) do
            local plugin_obj = plugin.get(name)
            if plugin_obj and plugin_obj.type == "auth" then
                if not plugins[name] then
                    plugins[name] = {
                        nodes = {},
                        len = 0,
                        conf_version = consumers.conf_version
                    }
                end

                local consumer, err = construct_consumer_data(val, name, config)
                if not consumer then
                    core.log.error("failed to construct consumer data for plugin ",
                                   name, ": ", err)
                    goto CONTINUE
                end

                plugins[name].len = plugins[name].len + 1
                core.table.insert(plugins[name].nodes, plugins[name].len, consumer)
            end
        end

        ::CONTINUE::
    end

    return plugins
end


function create_consume_cache(consumers_conf, key_attr)
    local consumer_names = {}

    for _, consumer in ipairs(consumers_conf.nodes) do
        local new_consumer = get_filled_consumer(consumer)
        consumer_names[new_consumer.auth_conf[key_attr]] = new_consumer
    end

    return consumer_names
end

end


local function new_plugin_index(plugin_name)
    return {
        plugin_name = plugin_name,
        conf_version = 0,
        full_sync_version = 0,
        built = false,
        invalid = false,
        dirty_keys = {},
        nodes = {},
        len = 0,
        pos_by_etcd_key = {},
        by_etcd_key = {},
        lookup_maps = {},
    }
end


local function clear_plugin_index(index)
    index.conf_version = 0
    index.full_sync_version = 0
    index.built = false
    index.invalid = false
    index.dirty_keys = {}
    index.nodes = {}
    index.len = 0
    index.pos_by_etcd_key = {}
    index.by_etcd_key = {}
    index.lookup_maps = {}
end


local function add_lookup_member(lookup_map, auth_key, etcd_key)
    if auth_key == nil or not etcd_key then
        return
    end

    local members = lookup_map.members[auth_key]
    if not members then
        members = {}
        lookup_map.members[auth_key] = members
    end

    members[etcd_key] = true
end


local function remove_lookup_member(lookup_map, auth_key, etcd_key)
    if auth_key == nil or not etcd_key then
        return
    end

    local members = lookup_map.members[auth_key]
    if not members then
        return
    end

    members[etcd_key] = nil
    if next(members) == nil then
        lookup_map.members[auth_key] = nil
    end
end


local function refresh_lookup_winner(index, lookup_map, auth_key)
    if auth_key == nil then
        return true
    end

    local members = lookup_map.members[auth_key]
    if not members then
        lookup_map.values[auth_key] = nil
        return true
    end

    local winner
    local winner_pos = 0
    for etcd_key in pairs(members) do
        local consumer = index.by_etcd_key[etcd_key]
        local pos = index.pos_by_etcd_key[etcd_key]
        if not consumer or not pos then
            return nil, "failed to locate consumer by etcd key: " .. tostring(etcd_key)
        end

        if pos >= winner_pos then
            winner = consumer
            winner_pos = pos
        end
    end

    if not winner then
        lookup_map.members[auth_key] = nil
        lookup_map.values[auth_key] = nil
        return true
    end

    lookup_map.values[auth_key] = get_filled_consumer(winner)
    return true
end


local function sync_lookup_maps(index, old_consumer, new_consumer)
    local etcd_key = old_consumer and old_consumer._etcd_key or new_consumer._etcd_key

    for key_attr, lookup_map in pairs(index.lookup_maps) do
        local old_key = old_consumer and get_filled_consumer(old_consumer).auth_conf[key_attr]
        local new_key = new_consumer and get_filled_consumer(new_consumer).auth_conf[key_attr]

        if old_key and old_key ~= new_key then
            remove_lookup_member(lookup_map, old_key, etcd_key)
        end
        if new_key then
            add_lookup_member(lookup_map, new_key, etcd_key)
        end

        if old_key == new_key then
            local ok, err = refresh_lookup_winner(index, lookup_map, old_key)
            if not ok then
                return nil, err
            end
        else
            if old_key then
                local ok, err = refresh_lookup_winner(index, lookup_map, old_key)
                if not ok then
                    return nil, err
                end
            end
            if new_key then
                local ok, err = refresh_lookup_winner(index, lookup_map, new_key)
                if not ok then
                    return nil, err
                end
            end
        end
    end

    return true
end


local function upsert_index_consumer(index, consumer)
    local etcd_key = consumer._etcd_key
    local pos = index.pos_by_etcd_key[etcd_key]
    if pos then
        local old_consumer = index.nodes[pos]
        if not old_consumer then
            return nil, "failed to locate existing consumer at position: " .. pos
        end

        index.nodes[pos] = consumer
        index.by_etcd_key[etcd_key] = consumer
        return sync_lookup_maps(index, old_consumer, consumer)
    end

    index.len = index.len + 1
    pos = index.len
    index.nodes[pos] = consumer
    index.pos_by_etcd_key[etcd_key] = pos
    index.by_etcd_key[etcd_key] = consumer

    return sync_lookup_maps(index, nil, consumer)
end


local function remove_index_consumer(index, etcd_key)
    local pos = index.pos_by_etcd_key[etcd_key]
    if not pos then
        return true
    end

    local old_consumer = index.nodes[pos]
    local last_consumer = index.nodes[index.len]
    index.nodes[pos] = last_consumer
    index.nodes[index.len] = nil
    index.len = index.len - 1
    index.pos_by_etcd_key[etcd_key] = nil
    index.by_etcd_key[etcd_key] = nil

    if last_consumer and last_consumer._etcd_key ~= etcd_key then
        index.pos_by_etcd_key[last_consumer._etcd_key] = pos
    end

    local ok, err = sync_lookup_maps(index, old_consumer, nil)
    if not ok then
        return nil, err
    end

    if last_consumer and last_consumer._etcd_key ~= etcd_key then
        local filled_consumer = get_filled_consumer(last_consumer)
        for key_attr, lookup_map in pairs(index.lookup_maps) do
            local auth_key = filled_consumer.auth_conf[key_attr]
            if auth_key ~= nil then
                local refreshed, refresh_err = refresh_lookup_winner(index, lookup_map, auth_key)
                if not refreshed then
                    return nil, refresh_err
                end
            end
        end
    end

    return true
end


local function create_incremental_consume_cache(index, key_attr)
    local lookup_map = {
        values = {},
        members = {},
    }

    for _, consumer in ipairs(index.nodes) do
        local filled_consumer = get_filled_consumer(consumer)
        local auth_key = filled_consumer.auth_conf[key_attr]
        if auth_key ~= nil then
            add_lookup_member(lookup_map, auth_key, consumer._etcd_key)
            lookup_map.values[auth_key] = filled_consumer
        end
    end

    return lookup_map
end


local function rebuild_credential_keys_by_consumer()
    credential_keys_by_consumer = {}

    if not consumers or not consumers.values then
        credential_keys_full_sync_version = consumers and consumers.full_sync_version or 0
        return
    end

    for _, val in ipairs(consumers.values) do
        if type(val) == "table" and is_credential_etcd_key(val.key) then
            local consumer_name = get_consumer_name_from_credential_etcd_key(val.key)
            local credential_keys = credential_keys_by_consumer[consumer_name]
            if not credential_keys then
                credential_keys = {}
                credential_keys_by_consumer[consumer_name] = credential_keys
            end

            credential_keys[get_consumer_short_key(val.key)] = true
        end
    end

    credential_keys_full_sync_version = consumers.full_sync_version or 0
end


local function build_incremental_plugin_index(plugin_name, index)
    clear_plugin_index(index)

    if consumers.values then
        for _, val in ipairs(consumers.values) do
            if type(val) == "table" then
                local plugin_conf = val.value.plugins and val.value.plugins[plugin_name]
                if plugin_conf then
                    local consumer, err = construct_consumer_data(val, plugin_name, plugin_conf)
                    if consumer then
                        local ok, sync_err = upsert_index_consumer(index, consumer)
                        if not ok then
                            return nil, sync_err
                        end
                    else
                        core.log.error("failed to construct consumer data for plugin ",
                                       plugin_name, ": ", err)
                    end
                end
            end
        end
    end

    index.built = true
    index.conf_version = consumers.conf_version
    index.full_sync_version = consumers.full_sync_version or 0
    index.invalid = false
    index.dirty_keys = {}

    return index
end


local function reconcile_index_consumer(index, plugin_name, short_key)
    local val = consumers:get(short_key)
    local plugin_conf = val and val.value and val.value.plugins and val.value.plugins[plugin_name]
    if not plugin_conf then
        return remove_index_consumer(index, short_key)
    end

    local consumer, err = construct_consumer_data(val, plugin_name, plugin_conf)
    if not consumer then
        if is_credential_etcd_key(val.key) then
            return remove_index_consumer(index, short_key)
        end

        return nil, err
    end

    return upsert_index_consumer(index, consumer)
end


local function apply_incremental_plugin_index(index, plugin_name)
    local dirty_keys = index.dirty_keys
    if next(dirty_keys) == nil then
        index.conf_version = consumers.conf_version
        index.full_sync_version = consumers.full_sync_version or 0
        return index
    end

    local keys_to_process = {}
    for short_key in pairs(dirty_keys) do
        keys_to_process[short_key] = true

        if is_consumer_short_key(short_key) then
            local consumer_name = get_consumer_name_from_short_key(short_key)
            local credential_keys = credential_keys_by_consumer[consumer_name]
            if credential_keys then
                for credential_key in pairs(credential_keys) do
                    keys_to_process[credential_key] = true
                end
            end
        end
    end

    index.dirty_keys = {}

    for short_key in pairs(keys_to_process) do
        local ok, err = reconcile_index_consumer(index, plugin_name, short_key)
        if not ok then
            index.invalid = true
            return nil, err
        end
    end

    index.conf_version = consumers.conf_version
    index.full_sync_version = consumers.full_sync_version or 0

    return index
end


_M.filter_consumers_list = filter_consumers_list

function _M.get_consumer_key_from_credential_key(key)
    local uri_segs = core.utils.split_uri(key)
    return "/consumers/" .. uri_segs[3]
end


function _M.plugin(plugin_name)
    if incremental_consumer_index_enabled then
        local plugin_obj = plugin.get(plugin_name)
        if not plugin_obj or plugin_obj.type ~= "auth" then
            return nil
        end

        if credential_keys_full_sync_version ~= (consumers.full_sync_version or 0) then
            rebuild_credential_keys_by_consumer()
        end

        local index = plugin_indexes[plugin_name]
        if not index then
            index = new_plugin_index(plugin_name)
            plugin_indexes[plugin_name] = index
        end

        if not index.built or index.invalid or
           index.full_sync_version ~= (consumers.full_sync_version or 0) then
            local _, err = build_incremental_plugin_index(plugin_name, index)
            if err then
                core.log.error("failed to build consumer index for plugin ",
                               plugin_name, ": ", err)
                return nil
            end
        elseif index.conf_version ~= consumers.conf_version then
            local _, err = apply_incremental_plugin_index(index, plugin_name)
            if err then
                core.log.error("failed to apply consumer index for plugin ",
                               plugin_name, ": ", err)

                local _, rebuild_err = build_incremental_plugin_index(plugin_name, index)
                if rebuild_err then
                    core.log.error("failed to rebuild consumer index for plugin ",
                                   plugin_name, ": ", rebuild_err)
                    return nil
                end
            end
        end

        return index
    end

    local plugin_conf = core.lrucache.global("/consumers",
                            consumers.conf_version, plugin_consumer)
    return plugin_conf[plugin_name]
end


function _M.consumers_conf(plugin_name)
    return _M.plugin(plugin_name)
end


-- attach chosen consumer to the ctx, used in auth plugin
function _M.attach_consumer(ctx, consumer, conf)
    ctx.consumer = consumer
    ctx.consumer_name = consumer.consumer_name
    ctx.consumer_group_id = consumer.group_id
    ctx.consumer_ver = conf.conf_version

    core.request.set_header(ctx, "X-Consumer-Username", consumer.username)
    core.request.set_header(ctx, "X-Credential-Identifier", consumer.credential_id)
    core.request.set_header(ctx, "X-Consumer-Custom-ID", consumer.custom_id)
end


function _M.consumers()
    if not consumers then
        return nil, nil
    end

    return filter_consumers_list(consumers.values), consumers.conf_version
end


function _M.consumers_kv(plugin_name, consumer_conf, key_attr)
    if incremental_consumer_index_enabled and consumer_conf then
        local lookup_map = consumer_conf.lookup_maps[key_attr]
        if not lookup_map then
            lookup_map = create_incremental_consume_cache(consumer_conf, key_attr)
            consumer_conf.lookup_maps[key_attr] = lookup_map
        end

        return lookup_map.values
    end

    local consumers = lrucache("consumers_key#" .. plugin_name, consumer_conf.conf_version,
        create_consume_cache, consumer_conf, key_attr)

    return consumers
end


function _M.find_consumer(plugin_name, key, key_value)
    local consumer
    local consumer_conf
    consumer_conf = _M.plugin(plugin_name)
    if not consumer_conf then
        return nil, nil, "Missing related consumer"
    end
    local consumers = _M.consumers_kv(plugin_name, consumer_conf, key)
    consumer = consumers[key_value]
    return consumer, consumer_conf
end


local function check_consumer(consumer, key)
    local data_valid
    local err
    if is_credential_etcd_key(key) then
        data_valid, err = check_schema(core.schema.credential, consumer)
    else
        data_valid, err = check_schema(core.schema.consumer, consumer)
    end
    if not data_valid then
        return data_valid, err
    end

    return plugin_checker(consumer, core.schema.TYPE_CONSUMER)
end


local function filter(consumer)
    if consumer.value and consumer.value.plugins then
        plugin.set_plugins_meta_parent(consumer.value.plugins, consumer)
    end

    if not incremental_consumer_index_enabled or not consumer.key then
        return
    end

    local short_key = get_consumer_short_key(consumer.key)
    if not short_key then
        return
    end

    if is_credential_etcd_key(consumer.key) then
        local consumer_name = get_consumer_name_from_credential_etcd_key(consumer.key)
        local credential_keys = credential_keys_by_consumer[consumer_name]

        if consumer.value then
            if not credential_keys then
                credential_keys = {}
                credential_keys_by_consumer[consumer_name] = credential_keys
            end

            credential_keys[short_key] = true
        elseif credential_keys then
            credential_keys[short_key] = nil
            if next(credential_keys) == nil then
                credential_keys_by_consumer[consumer_name] = nil
            end
        end
    end

    for _, index in pairs(plugin_indexes) do
        index.dirty_keys[short_key] = true
    end
end


function _M.init_worker()
    local err
    local local_conf = config_local.local_conf()
    incremental_consumer_index_enabled =
        core.table.try_read_attr(local_conf, "apisix", "enable_incremental_consumer_index") or false
    plugin_indexes = {}
    credential_keys_by_consumer = {}
    credential_keys_full_sync_version = 0

    local cfg = {
        automatic = true,
        checker = check_consumer,
        filter = filter
    }

    consumers, err = core.config.new("/consumers", cfg)
    if not consumers then
        error("failed to create etcd instance for fetching consumers: " .. err)
        return
    end
end

local function get_anonymous_consumer_from_local_cache(name)
    local anon_consumer_raw = consumers:get(name)

    if not anon_consumer_raw or not anon_consumer_raw.value or
    not anon_consumer_raw.value.id or not anon_consumer_raw.modifiedIndex then
        return nil, nil, "failed to get anonymous consumer " .. name
    end

    local anon_consumer = anon_consumer_raw.value
    anon_consumer.consumer_name = anon_consumer_raw.value.id
    anon_consumer.modifiedIndex = anon_consumer_raw.modifiedIndex

    local anon_consumer_conf = {
        conf_version = anon_consumer_raw.modifiedIndex
    }

    return anon_consumer, anon_consumer_conf
end


function _M.get_anonymous_consumer(name)
    local anon_consumer, anon_consumer_conf, err
    anon_consumer, anon_consumer_conf, err = get_anonymous_consumer_from_local_cache(name)

    return anon_consumer, anon_consumer_conf, err
end


return _M
