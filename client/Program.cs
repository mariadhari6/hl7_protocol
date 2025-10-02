using System.Net.Sockets;
using System.Text;
using DotNetEnv;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;

public class Client
{
  private static string TEXT = """
  Hello World
  Hello Dunia
  Hello Morasaurus
  Hello Dinoaurus
  Lorem Ipsum
  """;
  private static byte CR = 13;
  private static byte FS = 28;
  private static byte VT = 11;

  private static JObject? STRUCT;

  private static bool running = true;

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
  private static void LogPacket(byte[] packet)
  {
    string text = Encoding.Latin1.GetString(packet);
    Log.Information(text.Replace("\r", "<CR>").Replace(((char)FS).ToString(), "<FS>").Replace(((char)VT).ToString(), "<VT>"));
  }
  private static JObject ParseContentText(JArray structure, string line, string sep = "|")
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

        JObject childObj = ParseContentText(new JArray(fieldDef), childLine, "^");
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

      // we only use the first key in this structure
      string key = keys[0];
      JToken tokenNode = itemObj[key]!;

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
        var structureChild = (JArray)itemObj[key]!;
        var dataChild = data[key];
        JToken tokenChild = dataChild;

        if (dataChild != null)
        {
          if (tokenChild.Type == JTokenType.Array)
          {
            List<string> tokenList = new();
            for (int i = 0; i < ((JArray)dataChild).Count; i++)
            {
              tokenList.Add(GetContentText(structureChild, dataChild[i], sep: "^", sequence: sequence));
            }
            token = string.Join('\\', tokenList);
          }
          else
          {
            token = GetContentText(structureChild, dataChild, sep: "^", sequence: sequence);
          }
        }
      }
      else if (tokenNode.Type == JTokenType.Object)
      {
        // nested JObject
        token = GetContentText(new JArray(tokenNode), data[key], sep: "^", sequence: sequence);
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


  private static string PacketToString(byte[] packet)
  {
    string rawText = Encoding.Latin1.GetString(packet.Skip(1).Take(packet.Length - 3).ToArray()).Replace("\r", "<CR>").Replace(((char)FS).ToString(), "<FS>").Replace(((char)VT).ToString(), "<VT>");
    string[] lines = rawText.Split("<CR>").Where(s => s != "").ToArray();

    return string.Join("\n", lines);
  }
  private static void HandleConnection(NetworkStream stream)
  {
    try
    {
      byte[] buffer = new byte[1024];
      int byteCount;
      List<byte> incoming = new();

      while ((byteCount = stream.Read(buffer, 0, buffer.Length)) > 0)
      {
        incoming.AddRange(buffer.Take(byteCount));
        if (incoming.Contains(FS))
        {
          break;
        }
      }
      string message = PacketToString(incoming.ToArray());
      Log.Information(message);
      JObject? receivedData = ClearJObjectData(ConvertTextToJSON(message)!);
      Log.Information(receivedData.ToString(Formatting.Indented));

    }
    catch (Exception e)
    {
      Log.Error("Connection error: " + e.Message);
      running = false;
    }
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

      result.Add(ParseContentText(structure!, value));
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

      Log.Information("Load structure file json...");
      string structureStrig = File.ReadAllText("hl7_structure.json");
      STRUCT = JsonConvert.DeserializeObject(structureStrig) as JObject;

      string dataString = File.ReadAllText("calibration_test.json");
      JArray? data = JsonConvert.DeserializeObject(dataString) as JArray;

      TEXT = ConvertJSONToText((JObject)STRUCT!.DeepClone(), (JObject)data![0]);
      Console.WriteLine(TEXT);

      string TCP_HOST = Environment.GetEnvironmentVariable("TCP_HOST") ?? "127.0.0.1";
      int PORT = int.Parse(Environment.GetEnvironmentVariable("TCP_PORT") ?? "5000");

      TcpClient client = new(TCP_HOST, PORT);
      NetworkStream stream = client.GetStream();

      byte[] dataBytes = GetBytes(TEXT);
      LogPacket(dataBytes);
      stream.Write(dataBytes);

      Thread t = new(() => HandleConnection(stream));
      t.Start();
      while (running)
      {
        Thread.Sleep(100);
      }
      Log.Information("Client Disconnected.");
      client.Close();
    }
    catch (Exception e)
    {
      Log.Error("Error message: " + e.Message);
    }
  }
}