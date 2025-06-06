local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

local raw_items = {}
for _, s in pairs(cjson.decode(os.getenv("raw_items"))) do
  raw_items[s] = true
end

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

local item_definitions = {
  ["^https?://www%.(rfa%.org/.+)$"]="article",
  ["^https?://www%.(wainao%.me/.+)$"]="article",
  ["^https?://www%.(benarnews%.org/.+)$"]="article",
  ["^https?://[^/]*arcpublishing%.com/api/v1/ansvideos/findByUuid%?uuid=([0-9a-zA-Z%-]+)"]="video",
  ["^https?://([^/]+/resizer/.+)$"]="asset",
  ["^https?://([^/]*arcpublishing%.com/.+)$"]="asset",
  ["^https?://([^/]*amazonaws%.com/.+)$"]="asset",
  ["^https?://([^/]*cloudfront%.net/.+)$"]="asset",
  ["^https?://([^/]*akamaized%.net/.+)$"]="asset",
  ["^https?://([^/]*arc%.pub/.+)$"]="asset",
  ["^https?://(screenshots%.[^/]+/.+)$"]="asset",
  ["^https?://([^/]*cdn%.acast%.com/.+)$"]="asset",
  ["^https?://(audio%.[^/]+/.+)$"]="asset"
}

find_item = function(url)
  if ids[url] then
    return {}
  end
  local value = nil
  local type_ = nil
  local finds = {}
  for pattern, name in pairs(item_definitions) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      table.insert(finds, {
        ["value"]=value,
        ["type"]=type_
      })
    end
  end
  return finds
end

set_item = function(url)
  finds = find_item(url)
  local accepted = 0
  for _, found in pairs(finds) do
    local newcontext = {}
    local new_item_type = found["type"]
    local new_item_value = found["value"]
    if new_item_type == "article" then
      local site, path = string.match(new_item_value, "^([^/]+)%.[a-z]+/(.+)$")
      newcontext["site"] = site
      newcontext["path"] = path
      for s in string.gmatch(path, "([^/%?&;]+)") do
        if ids[string.lower(s)] then
          return nil
        end
      end
      new_item_value = site .. ":" .. path
    end
    new_item_name = new_item_type .. ":" .. new_item_value
    if raw_items[new_item_name]
      and new_item_name ~= item_name then
      accepted = accepted + 1
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      if item_type == "article" then
        local slug = string.match(item_value, "[^%?&;]+/([^/%?&;]+)")
        if string.match(slug, "/$") then
          slug = string.match(slug, "^(.+)/$")
        end
        if string.match(slug, "%.html$") then
          slug = string.match(slug, "^(.+)%.html$")
        end
        ids[string.lower(slug)] = true
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
  assert(accepted <= 1)
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

is_allowed_site = function(url)
  local domain = string.match(url, "^https?://([^/]*)")
  local known_domains = {
    "^www%.rfa%.org$",
    "^rfa%.org$",
    "^www%.wainao%.me$",
    "^wainao%.me$",
    "^shorthand%.wainao%.me$",
    "^www%.benarnews%.org$",
    "^benarnews%.org$",
    "^screenshots%.",
    "^audio%.",
    "^tags%.",
    "^ssc%.",
  }
  for _, s in pairs(known_domains) do
    if string.match(domain, s) then
      return true
    end
  end
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "^https?://shorthand%.wainao%.me/.")
    or (
      string.match(url, "^https?://[^/]+%.acast%.com/.")
      and (
        not parenturl
        or not string.match(parenturl, "^https?://[^/]*acast%.com/")
      )
    ) then
    return true
  end

  if string.match(url, "^https?://radiofreeasia%.arcpublishing%.com/goldfish/")
    or url == "https://radiofreeasia.video-player.arcpublishing.com/"
    or string.match(url, "\\\"")
    or string.match(url, "^https?://[^/]+/%*$")
    or string.match(url, "^https?://[^/]+/pf/") then
    return false
  end

  if not parenturl
    or not string.match(parenturl, "^https?://[^/]*dwcdn%.net/") then
    local a = string.match(url, "^https?://datawrapper%.dwcdn%.net/([0-9a-zA-Z]+)/")
    if a then
      ids[string.lower(a)] = true
    end
  end

  local good_site = is_allowed_site(url)

  if not good_site
    and (
      string.match(url, "^https?://[^/]*rfa%.org/")
      or string.match(url, "^https?://[^/]*wainao%.me/")
      or string.match(url, "^https?://[^/]*benarnews%.org/")
    ) then
    error("Found unexpected site " .. url .. ".")
  end

  if good_site then
    for s in string.gmatch(string.match(url, "^https?://[^/]+/?(.*)$"), "([^/%?&;]*)") do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  local found_asset = true
  local skip = false
  for pattern, type_ in pairs(item_definitions) do
    match = string.match(url, pattern)
    if match and type_ == "asset" then
      found_asset = true
    end
    if match
      and (
        type_ ~= "asset"
        or item_type ~= "video"
      )
      and (
        type_ ~= "article"
        or not string.match(match, "^[^/]+/resizer/")
      ) then
      local new_item = type_ .. ":" .. match
      if type_ == "article" then
        local site, path = string.match(match, "^[^/]-([a-z]+)%.[a-z]+/(.+)$")
        assert(site == "rfa" or site == "wainao" or site == "benarnews")
        new_item = type_ .. ":" .. site .. ":" .. path
      end
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if found_asset and item_type == "video" then
    return true
  end

  if not good_site
    and not string.match(url, "^https?://[^/]*dwcdn%.net/")
    and not string.match(url, "^https?://[^/]*datawrapper%.de/")
    and not string.match(url, "^https?://[^/]*acast%.com/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  local temp = string.match(url, "^(.+)%.html$")
  if temp then
    url = temp
  end

  for _, pattern in pairs({
    "([0-9]+)",
    "([0-9a-zA-Z_]+)",
    "([^/%?&;]+)",
    "([^/%?&;%.]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url]
    and not string.match(parent["url"], "%.css$") then
    addedtolist[url] = true
    return true
  end

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if string.match(newurl, "%s") then
      for s in string.gmatch(newurl, "([^%s]+)") do
        check(s)
      end
      return nil
    end
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  if string.match(url, "^https?://[^/]+/resizer/") then
    local newurl = string.match(url, "^([^%?]+)")
    local auth = string.match(url, "[%?&](auth=[^&]+)")
    if auth then
      newurl = newurl .. "?" .. auth
    end
    check(newurl)
    local arc_path = string.match(newurl, "/arc%-[^%-]+%-(radiofreeasia/[^%?]+)")
    if arc_path then
      check("https://cloudfront-us-east-1.images.arcpublishing.com/" .. arc_path)
    end
    local inner_url = string.match(newurl, "/(https?%%3A%%2F%%2F[^%?&]+)")
    if inner_url then
      inner_url = urlparse.unescape(inner_url)
      check(inner_url)
    end
  end

  local skip_url = false
  if item_type == "article"
    and not string.match(url, "%.html?$")
    and not string.match(url, "/$") then
    local to_check = {"css", "images", "js", "fonts", "static"}
    if string.match(url, "^https?://shorthand%.wainao%.me/.") then
      table.insert(to_check, "assets")
    end
    for _, s in pairs(to_check) do
      if string.match(url, "./" .. s .. "/")
        and ids[string.lower(string.match(url, "([^/]+)/" .. s .. "/"))] then
        skip_url = true
        break
      end
    end
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset"
    and not skip_url
    and not string.match(url, "^https?://[^/]*acast%.com/.+%.mp3")
    and (
      item_type ~= "video"
      or string.match(url, "findByUuid")
      or string.match(url, "%.m3u8")
    ) then
    html = read_file(file)
    local acast_1, acast_2 = string.match(url, "^https?://embed%.acast%.com/([a-f0-9]+)/([a-f0-9]+)")
    if acast_1 and acast_2 then
      ids[acast_2] = true
      check("https://feeder.acast.com/api/v1/shows/" .. acast_1 .. "/episodes/" .. acast_2 .. "?showInfo=true")
      check("https://sphinx.acast.com/p/open/s/" .. acast_1 .. "/e/" .. acast_2 .. "/media.json")
    end
    if item_type == "video" then
      check("https://radiofreeasia-prod-cdn.video-api.arcpublishing.com/api/v1/ansvideos/findByUuid?uuid=" .. item_value)
    end
    if string.match(url, "^https?://datawrapper%.dwcdn%.net/[^/]+/[0-9]+/$") then
      local version = string.match(url, "([0-9]+)/$")
      for i=1,tonumber(version) do
        check(urlparse.absolute(url, "../" .. tostring(i) .. "/"))
      end
    end
    if string.match(url, "^https?://datawrapper%.dwcdn%.net/.+/$") then
      local data = string.match(html, "window%.__DW_SVELTE_PROPS__%s*=%s*JSON%.parse%((\".-\")%);\n")
      if data then
        local json = cjson.decode(cjson.decode(data))
        html = html .. flatten_json(json)
        for _, d in pairs(json["assets"]) do
          check(urlparse.absolute(url, d["url"]))
        end
      end
    end
    html = string.gsub(html, "Fusion%.[a-zA-Z0-9]+=({.-});[a-zA-Z<]", function (s)
      local function process_json(data)
        if type(data) == "table" then
          if data["type"] == "video"
            and data["_id"]
            and data["_id"] ~= cjson.null then
            discover_item(discovered_items, "video:" .. data["_id"])
            if not data["streams"] or data["streams"] == cjson.null then
              data["streams"] = {}
            end
            for _, stream_data in pairs(data["streams"]) do
              local matched = false
              for pattern, type_ in pairs(item_definitions) do
                if type_ == "asset"
                  and string.match(stream_data["url"], pattern) then
                  matched = true
                  break
                end
              end
              if matched then
                stream_data["url"] = nil
              end
            end
          end
          for k, v in pairs(data) do
            if type(v) == "table" then
              process_json(v)
            end
          end
        end
      end
      local json = cjson.decode(s)
      process_json(json)
      return cjson.encode(json)
    end)
    if string.match(url, "%.m3u8") then
      for line in string.gmatch(html, "([^\n]+)") do
        if not string.match(line, "^#") then
          local newurl = urlparse.absolute(url, line)
          ids[newurl] = true
          check(newurl)
        end
      end
      for newurl in string.gmatch(html, "[uU][rR][lLiI]=\"([^\"]+)\"") do
        newurl = urlparse.absolute(url, newurl)
        ids[newurl] = true
        check(newurl)
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 404
    and http_stat["statcode"] ~= 301
    and (
      http_stat["statcode"] ~= 302
      or not string.match(url["url"], "^https?://[^/]*acast%.com/")
    ) then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300
    and not string.match(url["url"], "^https?://[^/]*dwcdn%.net/") then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 8
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["radiofreeasia-3wpedoq3aldusmpw"] = discovered_items,
    ["urls-mts4b8qb6wrrterr"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


