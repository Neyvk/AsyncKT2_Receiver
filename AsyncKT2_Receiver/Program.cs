using System;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading.Tasks;

class Program
{
    static async Task Main()
    {
        using var pipeServer = new NamedPipeServerStream(
            "TestPipe",
            PipeDirection.In,
            1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);

        Console.WriteLine("Ожидание клиента...");
        await pipeServer.WaitForConnectionAsync();

        Console.WriteLine("Клиент подключён.");

        using var reader = new BinaryReader(pipeServer, Encoding.UTF8, true);

        while (true)
        {
            string type;

            try
            {
                type = reader.ReadString();
            }
            catch (EndOfStreamException)
            {
                Console.WriteLine("Клиент отключился.");
                break;
            }

            if (type == "text")
            {
                string message = reader.ReadString();

                Console.WriteLine();
                Console.WriteLine("=== ТЕКСТ ===");
                Console.WriteLine(message);
                Console.WriteLine();
            }
            else if (type == "file")
            {
                string fileName = reader.ReadString();
                long fileSize = reader.ReadInt64();
                string filePipeName = reader.ReadString();

                _ = Task.Run(() =>
                    ReceiveFileAsync(filePipeName, fileName, fileSize));
            }
            else if (type == "shutdown")
            {
                Console.WriteLine("Получена команда завершения.");
                break;
            }
        }
    }

    static async Task ReceiveFileAsync(
        string pipeName,
        string fileName,
        long fileSize)
    {
        try
        {
            using var filePipe = new NamedPipeServerStream(
                pipeName,
                PipeDirection.In,
                1,
                PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous);

            Console.WriteLine($"Ожидание передачи файла {fileName}...");

            await filePipe.WaitForConnectionAsync();

            Directory.CreateDirectory("ReceivedFiles");

            string savePath = Path.Combine("ReceivedFiles", fileName);

            await using FileStream fs = new FileStream(
                savePath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                8192,
                true);

            byte[] buffer = new byte[8192];
            long remaining = fileSize;

            while (remaining > 0)
            {
                int read = await filePipe.ReadAsync(
                    buffer,
                    0,
                    (int)Math.Min(buffer.Length, remaining));

                if (read == 0)
                    throw new EndOfStreamException();

                await fs.WriteAsync(buffer, 0, read);

                remaining -= read;
            }

            Console.WriteLine();
            Console.WriteLine("=== ФАЙЛ ===");
            Console.WriteLine($"Имя: {fileName}");
            Console.WriteLine($"Размер: {fileSize} байт");
            Console.WriteLine($"Сохранён: {savePath}");
            Console.WriteLine();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка получения файла: {ex.Message}");
        }
    }
}