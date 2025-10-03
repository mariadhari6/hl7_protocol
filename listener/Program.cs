using System.Net;
using System.Net.Sockets;
using System.Text;
using DotNetEnv;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;

class TcpServer
{
  private static byte CR = 13;
  private static byte FS = 28;
  private static byte VT = 11;

  private static JObject? STRUCT;

  private static void LogPacket(byte[] packet)
  {
    string text = Encoding.Latin1.GetString(packet);
    // packet.Skip(1).Take(packet.Length - 3).ToArray() // remove VT, FS & CR
    Log.Information(text.Replace("\r", "<CR>").Replace(((char)FS).ToString(), "<FS>").Replace(((char)VT).ToString(), "<VT>"));
  }

  private static string PacketToString(byte[] packet)
  {
    string rawText = Encoding.Latin1.GetString(packet.Skip(1).Take(packet.Length - 3).ToArray()).Replace("\r", "<CR>").Replace(((char)FS).ToString(), "<FS>").Replace(((char)VT).ToString(), "<VT>");
    string[] lines = rawText.Split("<CR>").Where(s => s != "").ToArray();

    return string.Join("\n", lines);
  }

  private static JObject ParseContentText(JArray structure, string line, string sep = "|", string sequence = "")
  {
    var obj = new JObject();
    string[] tokens = line.Split(sep);

    int index = 0;
    foreach (var field in structure)
    {
      JObject fieldObj = (JObject)field;
      string key = fieldObj.Properties().First().Name;
      JToken fieldDef = fieldObj[key];

      if (fieldDef.Type == JTokenType.String) // simple field
      {
        obj[key] = index < tokens.Length ? tokens[index] : "";
        index++;
      }
      else if (fieldDef.Type == JTokenType.Array) // nested array (like Universal Test ID)
      {
        string childLine = index < tokens.Length ? tokens[index] : "";
        index++;
        List<string> listLine = childLine.Split('\\').ToList();
        JArray childStructure = (JArray)fieldDef;
        if (listLine.Count > 1)
        {
          JArray childArray = new JArray();
          for (int i = 0; i < listLine.Count; i++)
          {
            JObject childObj = ParseContentText(childStructure, listLine[i], "^");
            childArray.Add(childObj);
          }
          obj[key] = childArray;
        }
        else
        {
          JObject childObj = ParseContentText(childStructure, childLine, "^");
          obj[key] = childObj;
        }
      }
      else if (fieldDef.Type == JTokenType.Object)
      {
        // nested object
        string childLine = index < tokens.Length ? tokens[index] : "";
        index++;

        JArray childArr = new JArray(fieldDef);
        int idx = Int32.Parse(sequence) - 1;


        var childField = (JObject)fieldDef;

        int lastIndex = childField.Properties().Count() - 1;


        var property = childField.Properties().ElementAtOrDefault(idx < lastIndex ? idx : lastIndex);
        var filtered = new JObject { [property!.Name] = property.Value };
        childArr[0] = filtered;

        JObject childObj = ParseContentText(childArr, childLine, "^", sequence: sequence);
        obj[key] = childObj;
      }
    }

    return obj;
  }
  private static JArray ParseResultText(JArray structure, List<string> lines, string sep = "|")
  {
    var arr = new JArray();
    foreach (var line in lines)
    {
      arr.Add(ParseContentText(structure, line, sep));
    }
    return arr;
  }

  private static string GetContentText(JArray structure, dynamic data, string sep = "|", string sequence = "1")
  {
    var listText = new List<string>();

    foreach (var item in structure)
    {
      JObject itemObj = (JObject)item;
      List<string> keys = itemObj.Properties().Select(p => p.Name).ToList();

      string key = keys[0];
      JToken tokenNode = itemObj[key]!;
      string token = "";

      if (tokenNode.Type == JTokenType.String ||
          tokenNode.Type == JTokenType.Integer ||
          tokenNode.Type == JTokenType.Boolean ||
          tokenNode.Type == JTokenType.Float)
      {
        token = tokenNode?.ToString() ?? "";
        if (string.IsNullOrEmpty(token))
          token = data[key]?.ToString() ?? "";
      }
      else if (tokenNode.Type == JTokenType.Array)
      {
        var structureChild = (JArray)itemObj[key]!;
        var dataChild = data[key];
        if (dataChild != null && dataChild.Type == JTokenType.Array)
        {
          List<string> tokenList = new();
          foreach (var d in dataChild)
          {
            tokenList.Add(GetContentText(structureChild, d, sep: "^", sequence: sequence));
          }
          token = string.Join("\\", tokenList);
        }
      }
      else if (tokenNode.Type == JTokenType.Object)
      {
        var structureChild = (JObject)tokenNode;
        var dataChild = data[key] as JObject;

        if (dataChild != null)
        {
          int index = 1;
          var lines = new List<string>();

          foreach (var childProp in structureChild.Properties())
          {
            string childKey = childProp.Name;
            var childNode = childProp.Value;

            if (childKey == "Sample Test")
            {
              // khusus Sample Test → array jadi beberapa baris
              var structureArr = (JArray)childNode;
              var dataArr = dataChild[childKey];

              if (dataArr != null && dataArr.Type == JTokenType.Array)
              {
                foreach (var d in dataArr)
                {
                  string testValue = GetContentText(structureArr, d, sep: "^", sequence: sequence);
                  lines.Add($"DSP|{index}||{testValue}|||");
                  index++;
                }
              }
            }
            else
            {
              string value = "";
              if (childNode.Type == JTokenType.String)
                value = dataChild[childKey]?.ToString() ?? "";
              else if (childNode.Type == JTokenType.Object)
                value = GetContentText(new JArray(childNode), dataChild[childKey], sep: "^", sequence: sequence);

              lines.Add($"DSP|{index}||{value}|||");
              index++;
            }
          }

          // langsung return multiline string
          return string.Join("\n", lines);
        }
      }

      if (key == "Set ID")
      {
        token = sequence;
      }
      listText.Add(token);
    }
    return string.Join(sep, listText);
  }
  private static string GetHeaderText(JArray structure, dynamic data, string sep = "|")
  {
    var listText = new List<string>();

    foreach (var item in structure)
    {
      JObject itemObj = (JObject)item;
      List<string> keys = itemObj.Properties().Select(p => p.Name).ToList();

      // we only use the first key in this structure
      string key = keys[0];
      JToken tokenNode = itemObj[key];

      string token = "";

      if (tokenNode.Type == JTokenType.String ||
          tokenNode.Type == JTokenType.Integer ||
          tokenNode.Type == JTokenType.Boolean ||
          tokenNode.Type == JTokenType.Float)
      {
        // JValue
        token = tokenNode?.ToString() ?? "";
        if (string.IsNullOrEmpty(token))
        {
          // fallback ke data
          token = data[key]?.ToString() ?? "";
        }
      }
      else if (tokenNode.Type == JTokenType.Array)
      {
        // recursive call for nested structure
        var structureChild = (JArray)itemObj[key];
        var dataChild = data[key];
        JToken tokenChild = dataChild;

        if (dataChild != null)
        {
          if (tokenChild.Type == JTokenType.Array)
          {
            List<string> tokenList = new();
            for (int i = 0; i < ((JArray)dataChild).Count; i++)
            {
              tokenList.Add(GetHeaderText(structureChild, dataChild, "^"));
            }
            token = string.Join('\\', tokenList);
          }
          else
          {
            token = GetHeaderText(structureChild, dataChild, "^");
          }
        }
      }
      else if (tokenNode.Type == JTokenType.Object)
      {
        // nested JObject (rare case in your sample)
        token = GetHeaderText(new JArray(tokenNode), data[key], sep: "^");
      }

      listText.Add(token);
    }

    return string.Join(sep, listText);
  }
  private static string ConvertJSONToText(JObject structure, JObject data)
  {
    List<string> lines = [];

    // Message Header (MSH)
    if (structure["Header"]?["fields"] is JArray headerStructure && data["Header"] is JObject dataHeader)
    {
      lines.Add(GetHeaderText(headerStructure, dataHeader));
      structure.Remove("Header");
    }
    foreach (var item in structure.Properties())
    {
      if (item.Value?["fields"] is JArray objFields)
      {
        if (data[item.Name.ToString()] is JObject objItem)
        {
          lines.Add(GetContentText(objFields, objItem, sequence: "1"));
        }
        else if (data[item.Name.ToString()] is JArray listItem)
        {
          for (int i = 0; i < listItem.Count; i++)
          {
            if (listItem[i] is JObject dataItem)
            {
              lines.Add(GetContentText(objFields, dataItem, sequence: (i + 1).ToString()));
            }
          }
        }
      }
    }
    return string.Join("\n", lines);
  }

  private static JObject? ConvertTextToJSON(string message)
  {
    List<string> lines = [.. message.Split("\n")];
    JArray? result = new JArray();

    foreach (var (value, index) in lines.Select((value, index) => (value, index)))
    {
      string recordType = value[..3].ToString();

      JArray? structure = STRUCT!.Properties()
                                 .Where(p => p.Value["code"] != null && p?.Value?["code"]?.ToString() == recordType)
                                 .Select(p => p.Value["fields"])
                                 .FirstOrDefault() as JArray;

      result.Add(ParseContentText(structure!, value, sequence: value.Split("|")[1]));
    }

    var recordTypes = result.Select(p => p["Record type"]?.ToString()).Where(v => !string.IsNullOrEmpty(v)).Distinct().ToList();
    JObject? record = new JObject();

    foreach (var rt in recordTypes)
    {
      string? key = STRUCT!.Properties()
                           .Where(p => p.Value["code"] != null && p?.Value?["code"]?.ToString() == rt)
                           .Select(p => p.Name.ToString())
                           .FirstOrDefault();

      JArray? items = new(result.Where(r => r["Record type"] != null && r["Record type"]!.ToString() == STRUCT![key!]!["code"]!.ToString()));
      JObject firstItem = (JObject)items[0];
      bool hasSequence = firstItem.Properties().Any(p => p.Name == "Set ID");
      if (items.Count > 1 || hasSequence)
      {
        record[key!] = items;
      }
      else
      {
        record[key!] = firstItem;
      }
    }
    return record;
  }
  private static JObject ClearJObjectData(JObject data)
  {
    // copy daftar property biar aman dari "collection modified" error
    var keys = data.Properties().ToList();

    foreach (var prop in keys)
    {
      string keyName = prop.Name;

      // hapus jika key bernama [NONE]
      if (keyName == "[NONE]")
      {
        data.Remove(keyName);
        continue;
      }

      // kalau value berupa JObject, rekursif
      if (prop.Value.Type == JTokenType.Object)
      {
        data[keyName] = ClearJObjectData((JObject)prop.Value);
      }
      // kalau value berupa array, cek setiap item apakah JObject
      else if (prop.Value.Type == JTokenType.Array)
      {
        JArray arr = (JArray)prop.Value;
        for (int i = 0; i < arr.Count; i++)
        {
          if (arr[i].Type == JTokenType.Object)
          {
            arr[i] = ClearJObjectData((JObject)arr[i]);
          }
        }
      }
      // kalau value berupa string kosong
      else if (prop.Value.Type == JTokenType.String && string.IsNullOrEmpty(prop.Value.ToString()))
      {
        data.Remove(prop.Name);
      }
    }

    return data;
  }
  private static byte[] GetBytes(string text)
  {
    List<byte> packet = [VT];
    List<string> listMessage = text.Split("\n").ToList();

    foreach (string s in listMessage)
    {
      packet.AddRange([.. Encoding.Latin1.GetBytes(s), CR]);
    }
    packet.AddRange([FS, CR]);
    return [.. packet];
  }

  private static void SendResponse(JObject response, NetworkStream stream, string information = "Response")
  {
    string responseText = ConvertJSONToText((JObject)STRUCT!.DeepClone(), response);
    byte[] responseBytes = GetBytes(responseText);
    stream.Write(responseBytes);
    Log.Information($"=========== {information} ===========\n" + responseText);
  }


  private static JObject CreateResponseHeader(JObject receivedData, string observation_type = "ACK")
  {
    JObject messageHeader = (JObject)receivedData!["Header"]!.DeepClone();
    messageHeader["Message Type"]!["Observation Type"] = observation_type;
    messageHeader["Receiving Application"] = messageHeader["Sending Application"];
    messageHeader["Receiving Facility"] = messageHeader["Sending Facility"];
    messageHeader["Date/Time of Message"] = DateTime.Now.ToString("yyyyMMddHHmmss");

    messageHeader.Remove("Sending Application");
    messageHeader.Remove("Sending Facility");
    return messageHeader;
  }

  private static JObject? FindSampleData(string sample_no)
  {
    string sampleString = File.ReadAllText("sample_dsp.json");
    JArray? sampleData = JsonConvert.DeserializeObject(sampleString) as JArray;
    JObject? data = sampleData!.FirstOrDefault(p => p!["Sample No."]!.ToString() == sample_no) as JObject;
    return data;
  }
  private static void HandleProcessData(JObject receivedData, NetworkStream stream)
  {

    // Check if data is calibration test
    JObject? messageType = receivedData!["Header"]!["Message Type"] as JObject;

    // jika calibration test / observation result
    if (messageType!["Observation Type"]!.ToString() == "ORU") // jika observation result
    {
      JObject messageHeader = CreateResponseHeader(receivedData);

      JObject msa = new JObject
      {
        {"Record type", "MSA"},
        {"Acknowledgment Code", "AA"}, // accepted for default
        {"Message Control ID", messageHeader["Message Control ID"]},
        {"Text Message", "Message accepted"},
        {"Error Condition", "0"}
      };

      JObject response = new JObject
      {
        {"Header", messageHeader},
        {"Message Acknowledgment", msa}
      };

      SendResponse(response, stream);
    }
    else if (messageType!["Observation Type"]!.ToString() == "QRY") // jika message query
    {
      // Create Header
      JObject messageHeader = CreateResponseHeader(receivedData, "QCK");

      // Create MSA
      JObject msa = new JObject
      {
        {"Record type", "MSA"},
        {"Acknowledgment Code", "AA"}, // accepted for default
        {"Message Control ID", messageHeader["Message Control ID"]},
        {"Text Message", "Message accepted"},
        {"Error Condition", "0"}
      };


      // Create error field
      JObject err = new JObject
      {
        {"Record type", "ERR"},
        {"Error Code and Location", "0"}
      };

      // Create qak field
      JObject qak = new JObject
      {
        {"Record type", "QAK"},
        {"Query Tag", "SR"},
        {"Query Response Status", "OK"}
      };

      JObject response = new JObject
      {
        {"Header", messageHeader},
        {"Message Acknowledgment", msa},
        {"Error", err},
        {"Query Acknowledgment", qak}
      };

      SendResponse(response, stream, "Quick Query Response");


      // Find data to display to instrument
      string sample_id = receivedData!["Query Definition"]!["Who Subject Filter"]!.ToString();
      JObject? displayData = FindSampleData(sample_id);
      if (displayData != null)
      {
        messageHeader!["Message Type"]!["Observation Type"] = "DSR";
        messageHeader!["Message Type"]!["Trigger Event"] = "Q03";
        response["Display Data"] = displayData;
        response["Continuation Pointer"] = new JObject
        {
          {"Record type", "DSC"}
        };
        SendResponse(response, stream, "Display Response");
      }
    }
  }
  private static void HandleConnection(TcpClient client, NetworkStream stream)
  {
    string clientEndPoint = client.Client.RemoteEndPoint.ToString();
    try
    {
      Log.Information($"Client connected: {clientEndPoint}");
      byte[] buffer = new byte[1024];

      int byteCount;
      List<byte> allBuffer = new();
      while ((byteCount = stream.Read(buffer, 0, buffer.Length)) > 0)
      {
        allBuffer.AddRange(buffer.Take(byteCount));
        while (allBuffer.Contains(FS))
        {
          int fsIndex = allBuffer.IndexOf(FS);
          byte[] packet = allBuffer.Take(fsIndex + 2).ToArray();
          allBuffer.RemoveRange(0, fsIndex + 2);
          string message = PacketToString(packet);
          Log.Information("==================== Incoming Message ====================\n" + message);
          JObject? receivedData = ClearJObjectData(ConvertTextToJSON(message)!);
          HandleProcessData(receivedData, stream);
        }
      }
    }
    catch (Exception e)
    {
      Log.Error("Connection error: " + e.Message);
      Log.Information($"Client disconnected: {clientEndPoint}");
    }
    finally
    {
      client.Close();
      Log.Information($"Client {clientEndPoint} disconnected");
    }
  }
  public static void Main()
  {
    try
    {
      Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Information()
        .WriteTo.Console()
        .WriteTo.File("logs/myapp-.txt", rollingInterval: RollingInterval.Day)
        .CreateLogger();

      Log.Information("Load .env file...");
      Env.Load();
      int PORT = int.Parse(Environment.GetEnvironmentVariable("TCP_PORT") ?? "5000");

      Log.Information("Load structure file json...");

      string structureStrig = File.ReadAllText("hl7_structure.json");

      STRUCT = JsonConvert.DeserializeObject(structureStrig) as JObject;

      TcpListener listener = new(IPAddress.Any, PORT);

      listener.Start();
      Log.Information($"TCP Server started on port {PORT}");

      while (true)
      {
        TcpClient client = listener.AcceptTcpClient();
        NetworkStream stream = client.GetStream();

        // Handle client threaded
        Thread t = new(() => HandleConnection(client, stream));
        t.Start();
      }
    }
    catch (Exception ex)
    {
      Log.Error("Error message: " + ex.Message);
    }
  }
}